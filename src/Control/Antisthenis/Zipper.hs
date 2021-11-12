{-# LANGUAGE Arrows                    #-}
{-# LANGUAGE CPP                       #-}
{-# LANGUAGE ConstraintKinds           #-}
{-# LANGUAGE DeriveFoldable            #-}
{-# LANGUAGE DeriveFunctor             #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE FunctionalDependencies    #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE LambdaCase                #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE QuantifiedConstraints     #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TupleSections             #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE UndecidableInstances      #-}
{-# LANGUAGE ViewPatterns              #-}

module Control.Antisthenis.Zipper (mkProcId,runMech) where

import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Class.Writer
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.ATL.Transformers.Moore
import           Control.Antisthenis.ATL.Transformers.Writer
import           Control.Antisthenis.AssocContainer
import           Control.Antisthenis.Types
import           Control.Antisthenis.ZipperId
import           Control.Arrow                               hiding (first,
                                                              (>>>))
import           Control.Monad.Cont
import           Control.Monad.Identity
import           Control.Monad.Writer
import           Control.Utils.Free
import           Data.Foldable
import           Data.Maybe
import           Data.Utils.AShow
import           Data.Utils.Const
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.Unsafe
import           Data.Void


mkGgState :: ZipperParams w => [p] -> ZipState w p
mkGgState ps = ZipState { bgsInits = ps,bgsIts = acEmpty,bgsCoits = [] }

bgsReset :: ZipperParams w => ZipState w a -> ZipState w a
bgsReset bgs =
  mkGgState
  $ bgsInits bgs
  ++ (snd <$> toList (bgsIts bgs))
  ++ (snd <$> bgsCoits bgs)

-- Note that we use the free monad to allow for the case that the user
-- select a few consecutive commands before being askef for a new conf.
type ZCat w m =
  MealyArrow
    (WriterArrow (ZCoEpoch w) (Kleisli (FreeT (Cmds w) m)))
    (LConf w)
    (Zipper w (ArrProc w m))

-- | Build an evolving zipper. Provided a local conf this zipper may
-- rotate towards any (allowed) direction. After each rotation the
-- allowed directions are encapsulated in the return type. It is in
-- theory allowed that more than one steps are required (see Free
-- monad) but this one requests the local conf which can always be
-- resolved in one step.
mkZCatIni
  :: forall w m .
  (ZipperParams w,Monad m,AShowW w)
  => ZipperId
  -> [ArrProc w m]
  -> MooreCat
    (WriterArrow (ZCoEpoch w) (Kleisli (FreeT (Cmds w) m)))
    (LConf w)
    (Zipper w (ArrProc w m))
mkZCatIni zid [] = error $ "No arg values for operation: " ++ ashow zid
mkZCatIni zipId (a:as) = mkMooreCat iniZipper $ mkZCat mempty iniZipper
  where
    iniZipper =
      Zipper
      { zCursor = Identity (Nothing,a,a)
       ,zBgState = mkGgState as
       ,zRes = def
       ,zId = zidReset zipId
      }

-- Make a ziper evolution.
--
-- mkZCat does noth collect the coepochs, each iteration ONLY contains
-- the coepoch of the most recent cursor executed. The zipper state is
-- always dependent on a coepoch.
mkZCat
  :: (ZipperParams w,Monad m,AShowW w)
  => ZCoEpoch w
  -> Zipper w (ArrProc w m)
  -> MealyArrow
    (WriterArrow (ZCoEpoch w) (Kleisli (FreeT (Cmds w) m)))
    (LConf w)
    (Zipper w (ArrProc w m))
mkZCat coepoch0 (incrZipperUId -> prevz) =
  MealyArrow $ WriterArrow $ Kleisli $ \lconf -> FreeT $ do
    let Identity (_bnd0,inip,cursProc) = zCursor prevz
    -- "runningCursorOf" <<: (zId prevz,confCap lconf,bnd0)
    (coepoch,(p',val)) <- runArrProc cursProc lconf
    -- XXX: The bound returned does not respect the cap and also is
    -- not the bound emitted.
    let coepoch' = coepoch <> coepoch0
    -- "doneWithCursor" <<: (zId prevz,val,coepoch')
    -- Update the cursor value and rotate the zipper.
    return
      $ Free
      $ fmap return
      $ mapCmds (fmap (mempty,)) (coepoch',)
      $ case val of
        BndBnd bnd -> pushIt coepoch'
          $ mapCursor (const $ Const (bnd,inip,p')) prevz
        BndRes res -> pushCoit coepoch'
          $ mapCursor (const $ Const (Right res,p')) prevz
        BndErr err -> pushCoit coepoch'
          $ mapCursor (const $ Const (Left err,p')) prevz

mapCmds :: (ResetCmd a -> ResetCmd b) -> (a -> b) -> Cmds w a -> Cmds w b
mapCmds f g Cmds {..} =
  Cmds { cmdReset = f cmdReset,cmdItCoit = g <$> cmdItCoit }

type EvaledZipper w p v =
  Zipper' w (Const v) p (ZPartialRes w)

-- | This is almost a Mealy (Kleisly) arrow. From a gutted zipper (the
-- previous step was setting the cursor on an it) there are a couple
-- of ways to put the zipper together (either pull it or ini or reset
-- etc). Create the commands for that.
pushIt
  :: forall w m p .
  (Monad m,p ~ ArrProc w m,ZipperParams w,AShowW w)
  => ZCoEpoch w
  -> EvaledZipper w p (ZBnd w,InitProc p,p)
  -> Cmds w (ZCat w m,Zipper w p)
pushIt coepoch Zipper {zCursor = Const (bnd,inip,itp),..} =
  Cmds
  { cmdItCoit = cmdItCoit',cmdReset = DoReset (mkZCat mempty rzipper,rzipper) }
  where
    rzipper =
      Zipper
      { zCursor = Identity (Nothing,inip,inip)
       ,zBgState = bgsReset zBgState
       ,zRes = def
       ,zId = zidReset zId
      }
    cmdItCoit' = case bgsInits zBgState of
      []       -> CmdIt itEvolve
      ini:inis -> CmdItInit itEvolve $ iniEvolve ini inis
    iniEvolve ini inis = (mkZCat coepoch zipper,zipper)
      where
        zipper :: Zipper w p
        zipper =
          putRes
            (BndBnd bnd)
            (zRes
            ,Zipper
             { zCursor = Identity (Nothing,ini,ini)
              ,zBgState = zBgState
                 { bgsIts =
                     acUnlift $ acInsert bnd (inip,itp) $ bgsIts zBgState
                  ,bgsInits = inis
                 }
              ,zRes = ()
              ,zId = zId
             })
    -- Evolve by poping an iterator.
    itEvolve pop = (mkZCat coepoch zipper,zipper)
      where
        zipper =
          Zipper
          { zCursor = Identity (Just bnd',inip',itp')
           ,zBgState = zBgState { bgsIts = its' }
           ,zRes = zRes
           ,zId = zId
          }
        (bnd' :: ZBnd w,(inip',itp'),its') =
          pop $ acInsert bnd (inip,itp) (bgsIts zBgState)

pushCoit
  :: forall w m p .
  (Monad m,p ~ ArrProc w m,ZipperParams w,AShowW w)
  => ZCoEpoch w
  -> EvaledZipper w p (Either (ZErr w) (ZRes w),CoitProc p)
  -> Cmds w (ZCat w m,Zipper w p)
pushCoit coepoch Zipper {zCursor = Const newCoit,..} =
  Cmds { cmdItCoit = cmdItCoit'
        ,cmdReset = DoReset (mkZCat mempty rzipper,rzipper)
       }
  where
    newRes = either BndErr BndRes $ fst newCoit
    finZipper =
      Zipper
      { zCursor = Const coepoch
       ,zBgState = zBgState { bgsCoits = newCoit : bgsCoits zBgState }
       ,zRes = ()
       ,zId = zId
      }
    rzipper =
      Zipper
      { zCursor = Identity (Nothing,snd newCoit,snd newCoit)
       ,zBgState = bgsReset zBgState
       ,zRes = def
       ,zId = zidReset zId
      }
    cmdItCoit' = case (bgsInits zBgState,acNonEmpty $ bgsIts zBgState) of
      -- XXX: CmdFinished loses the coepoch
      ([],Nothing) -> CmdFinished $ ExZipper $ putRes newRes (zRes,finZipper)
      (ini:inis,Nothing) -> CmdInit $ evolve $ mkIniZ ini inis
      ([],Just nonempty) -> CmdIt $ \pop -> evolve
        $ let (k,(ini,it),its) = pop nonempty
              z = fromJustErr $ mkItZ (k,ini,it) its in z
      (ini:inis,Just nonempty) -> CmdItInit
        (\pop -> evolve
         $ let (k,(ini',it),its) = pop nonempty
         in fromJustErr $ mkItZ (k,ini',it) its)
        (evolve $ mkIniZ ini inis)
    -- Just return the zipper
    evolve zipper = (mkZCat coepoch zipper,zipper)
    mkIniBgs inis =
      zBgState { bgsCoits = newCoit : bgsCoits zBgState,bgsInits = inis }
    mkIniZ ini inis =
      putRes
        newRes
        (zRes
        ,Zipper
         { zCursor = Identity (Nothing,ini,ini)
          ,zBgState = mkIniBgs inis
          ,zRes = ()
          ,zId = zId
         })
    mkItBgs
      its = zBgState { bgsCoits = newCoit : bgsCoits zBgState,bgsIts = its }
    mkItZ (bnd,ini,it) its = replaceRes bnd newRes (zRes,z)
      where
        z =
          Zipper
          { zCursor = Identity (Just bnd,ini,it)
           ,zBgState = mkItBgs its
           ,zRes = ()
           ,zId = zId
          }

data ZState w m =
  ZState
  { zsCoEpoch   :: ZCoEpoch w
   ,zsZipper    :: Zipper w (ArrProc w m)
   ,zsItCmd     :: ZCat w m
   ,zsReset     :: ZCat w m
   ,zsResetLoop :: Bool
  }

type family MBF a :: * -> * where
  MBF (MealyArrow (WriterArrow w c) a b) =
    MB a (w,b) (ArrFunctor c)

mkProcId
  :: forall m w .
  (Monad m,ZipperParams w,AShowW w,Eq (ZCoEpoch w))
  => ZipperId
  -> [ArrProc w m]
  -> ArrProc w m
mkProcId zid procs = arrCoListen' $ mkMealy $ zNext zsIni
  where
    runCmdSequence
      :: FreeT (Cmds w) m ((ZCat w m,Zipper w (ArrProc w m)),ZCoEpoch w)
      -> m
        (Maybe (ZCat w m)
        ,Either
           (ZCoEpoch w,BndR w)
           ((ZCat w m,Zipper w (ArrProc w m)),ZCoEpoch w))
    runCmdSequence cmds = do
      (rstCmdM,r) <- zEvolutionStrategy cmds
      rstM <- forM rstCmdM $ \(DoReset (FreeT rst)) -> rst <&> \case
        Free _m -> error "Reset is expected to have a Pure layer"
        Pure ((resetCat,_resetZ),_coepochEmpty) -> resetCat
      return (rstM,r)
    fromArrow
      :: ZCat w m
      -> Conf w
      -> MBF
        (ArrProc w m)
        (FreeT (Cmds w) m ((ZCat w m,Zipper w (ArrProc w m)),ZCoEpoch w))
    fromArrow (MealyArrow m) cnf =
      fmap (FreeT . return) $ lift $ runFreeT $ runWriterT $ toKleisli m cnf
    zsIni :: ZState w m
    zsIni =
      ZState
      { zsCoEpoch = mempty
       ,zsZipper = iniZ
       ,zsReset = iniMealy
       ,zsItCmd = iniMealy
       ,zsResetLoop = True
      }
      where
        iniZ :: Zipper w (ArrProc w m)
        MooreMech iniZ iniMealy = mkZCatIni zid procs
    zNext :: ZState w m -> Conf w -> MBF (ArrProc w m) Void
    zNext zs@ZState {..} gconf = do
      let tr :: AShow a => String -> a -> MBF (ArrProc w m) ()
          tr msg arg =
            trZ
              (if zsResetLoop then "reset:" ++ msg else msg)
              zsZipper
              -- ["none"] -- print nothing
              []    -- print all
              arg
      -- let tr _ _ = return ()
      tr "zLocalizeConf" (confCap gconf,confEpoch gconf,zsCoEpoch)
      case zLocalizeConf zsCoEpoch gconf zsZipper of
        ShouldReset -> do
          tr "zLocalizeConf:reset" (zsCoEpoch,confEpoch gconf)
          let zs' = zsIni { zsItCmd = zsReset }
          (zNext zs' gconf :: MBF (ArrProc w m) Void)
        DontReset lconf -> do
          tr "zLocalizeConf:res" $ confCap lconf
          -- Try to find a value...
          case zEvolutionControl gconf zsZipper of
            Just res -> do
              -- Yield the value to the parent
              tr "result" (res,zsCoEpoch)
              yieldMB (zsCoEpoch,res) >>= zNext zs { zsResetLoop = False }
            Nothing -> do
              tr "no result" (zId zsZipper)
              -- Continue with the current computation
              cmdM <- fromArrow zsItCmd lconf
              (rstM,upd) <- lift $ runCmdSequence cmdM
              let zs' = zs { zsReset = fromMaybe zsReset rstM }
              -- Did the computation finish?
              case upd of
                Left (coepoch,res) -> do
                  -- Yes! Yield the  result to the parent
                  tr "resultFin" (res,coepoch)
                  yieldMB (coepoch,res) >>= zNext zs' { zsResetLoop = False }
                Right ((nxt,z),coepoch) -> do
                  -- No... there is more stuff to do
                  tr "cont-from:" (zipperShape z,coepoch)
                  let zs'' =
                        zs' { zsItCmd = nxt
                             ,zsZipper = z
                             ,zsCoEpoch = coepoch
                             ,zsResetLoop = False
                            }
                  zNext zs'' gconf
{-# INLINABLE mkProcId#-}
-- we want to be able to specialize this

runMech :: ArrowFunctor c => MealyArrow c a b -> a -> ArrFunctor c b
runMech (MealyArrow c) ini = snd <$> toKleisli c ini
