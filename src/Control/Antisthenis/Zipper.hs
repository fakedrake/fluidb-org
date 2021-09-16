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
import           Data.Profunctor
import           Data.Utils.AShow
import           Data.Utils.Const
import           Data.Utils.Debug
import           Data.Utils.Default
import           Data.Utils.EmptyF
import           Data.Utils.Unsafe


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
mkZCatIni zid [] = error $ "mul needs at least one value: " ++ ashow zid
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
    let Identity (_bnd,inip,cursProc) = zCursor prevz
    (coepoch,(p',val)) <- runArrProc cursProc lconf
    let coepoch' = coepoch <> coepoch0
    -- Update the cursor value and rotate the zipper.
    --
    -- XXX: There are cases where the coit process is const without
    -- lookup.
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
      { zCursor = EmptyF
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


mkProcId
  :: forall m w .
  (Monad m,ZipperParams w,AShowW w,Eq (ZCoEpoch w))
  => ZipperId
  -> [ArrProc w m]
  -> ArrProc w m
mkProcId zid procs = mkProc $ \gconf st@(DoReset rst,coepoch,MooreMech z it) -> do
  -- XXX: remember to reset the coepoch
  (coepoch',cmd) <- case zLocalizeConf coepoch gconf z of
    ShouldReset -> do
      -- Note that reset has one dummy level.
      lift (runFreeT rst) >>= \case
        Free _ -> error "Reset should have a shallow layer."
        Pure
          ((itR,zR),coepR_empty) -> case zLocalizeConf coepR_empty gconf zR of
          ShouldReset -> error $ "Double reset: " ++ ashow (zipperShape zR)
          DontReset
            lconf -> fmap (coepR_empty,) $ lift $ getCursorCmds itR lconf
    DontReset lconf -> do
      fmap (coepoch,) $ lift $ getCursorCmds it lconf
  (rst',res) <- lift $ runCmdSequence cmd
  case res of
    Left bndr -> do
      gconf' <- yieldMB (coepoch',bndr)
      return (gconf',st)
    Right ((nxt,z'),coepoch'') -> do
      gconf' <- case evolutionControl zprocEvolution gconf z' of
        Nothing -> return gconf
        Just r  -> yieldMB (coepoch'',r)
      return (gconf',(fromMaybe (DoReset rst) rst',coepoch'',MooreMech z' nxt))
  where
    runCmdSequence
      cmds = evolutionStrategy zprocEvolution $ FreeT $ return cmds
    getCursorCmds (MealyArrow m) cnf = runFreeT $ runWriterT $ toKleisli m cnf
    mkProc = putWriter . wrapMealy (resetIni,mempty,iniMoore)
      where
        resetIni = DoReset $ return ((iniMealy,iniZ),mempty)
        putWriter (MealyArrow m) = MealyArrow $ WriterArrow $ rmap go m
          where
            go (nxt,(coepoch,r)) = (coepoch,(putWriter nxt,r))
        iniMoore = mkZCatIni zid procs
        MooreMech iniZ iniMealy = iniMoore

runMech :: ArrowFunctor c => MealyArrow c a b -> a -> ArrFunctor c b
runMech (MealyArrow c) ini = snd <$> toKleisli c ini
