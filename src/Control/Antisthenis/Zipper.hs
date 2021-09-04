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

module Control.Antisthenis.Zipper
  (zSize
  ,mkZCat
  ,mkMachine
  ,mkProc
  ,mkProcId
  ,runMech) where

import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Common
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.ATL.Transformers.Moore
import           Control.Antisthenis.ATL.Transformers.Writer
import           Control.Antisthenis.AssocContainer
import           Control.Antisthenis.Types
import           Control.Arrow                               hiding (first)
import           Control.Monad.Identity
import           Control.Utils.Free
import           Data.Foldable
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
mkZCat'
  :: forall w m .
  (ZipperParams w,Monad m,AShow (ZBnd w))
  => ZipperId
  -> [ArrProc w m]
  -> MooreCat
    (WriterArrow (ZCoEpoch w) (Kleisli (FreeT (Cmds w) m)))
    (LConf w)
    (Zipper w (ArrProc w m))
mkZCat' zid [] = error $ "mul needs at least one value: " ++ ashow zid
mkZCat' zipId (a:as) = mkMooreCat iniZipper $ mkZCat iniZipper
  where
    iniZipper =
      Zipper
      { zCursor = Identity (Nothing,a,a)
       ,zBgState = mkGgState as
       ,zRes = def
       ,zId = zidReset zipId
      }

-- Make a ziper evolution.
mkZCat
  :: (ZipperParams w,Monad m,AShow (ZBnd w))
  => Zipper w (ArrProc w m)
  -> MealyArrow
    (WriterArrow (ZCoEpoch w) (Kleisli (FreeT (Cmds w) m)))
    (LConf w)
    (Zipper w (ArrProc w m))
mkZCat (incrZipperUId -> prevz) =
  MealyArrow $ WriterArrow $ Kleisli $ \lconf -> FreeT $ do
    let Identity (_bnd,inip,cursProc) = zCursor prevz
    (coepoch',(p',val)) <- runArrProc cursProc lconf
    -- Update the cursor value and rotate the zipper.
    -- traceM $ "Will now rotate: " ++ zId prevz
    runFreeT $ (coepoch',) <$> case val of
      BndBnd bnd -> pushIt $ mapCursor (const $ Const (bnd,inip,p')) prevz
      BndRes res -> pushCoit $ mapCursor (const $ Const (Right res,p')) prevz
      BndErr err -> pushCoit $ mapCursor (const $ Const (Left err,p')) prevz

-- | The cursor is already evaluated.
type EvaledZipper w p v =
  Zipper' w (Const v) p (ZPartialRes w)

-- | This is almost a Mealy (Kleisly) arrow. From a gutted zipper (the
-- previous step was setting the cursor on an it) there are a couple
-- of ways to put the zipper together (either pull it or ini or reset
-- etc). Create the commands for that.
pushIt :: forall w m p .
       (Monad m,p ~ ArrProc w m,ZipperParams w,AShow (ZBnd w))
       => EvaledZipper w p (ZBnd w,InitProc p,p)
       -> FreeT (Cmds w) m (ZCat w m,Zipper w p)
pushIt
  Zipper
  {zCursor = Const (bnd,inip,itp),..}   -- zid (bnd,inip,itp) oldRes bgs =
  =
  wrapFree
  $ Cmds { cmdItCoit = cmdItCoit'
          ,cmdReset = DoReset $ return (mkZCat rzipper,rzipper)
         }
  where
    rzipper =
      Zipper
      { zCursor = Identity (Nothing,inip,inip)
       ,zBgState = bgsReset zBgState
       ,zRes = def
       ,zId = zidReset zId
      }
    cmdItCoit' = DontReset $ case bgsInits zBgState of
      []       -> CmdIt itEvolve
      ini:inis -> CmdItInit itEvolve $ iniEvolve ini inis
    iniEvolve ini inis = return (mkZCat zipper,zipper)
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
    itEvolve pop = return (mkZCat zipper,zipper)
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

pushCoit :: forall w m p .
         (Monad m,p ~ ArrProc w m,ZipperParams w,AShow (ZBnd w))
         => EvaledZipper w p (Either (ZErr w) (ZRes w),CoitProc p)
         -> FreeT (Cmds w) m (ZCat w m,Zipper w p)
pushCoit Zipper {zCursor = Const newCoit,..} =
  wrapFree
  $ Cmds { cmdItCoit = cmdItCoit'
          ,cmdReset = DoReset $ return (mkZCat rzipper,rzipper)
         }
  where
    newRes = either BndErr BndRes $ fst newCoit
    resetCursor = Identity (Nothing,snd newCoit,snd newCoit)
    finZipper =
      Zipper
      { zCursor = EmptyF
       ,zBgState = zBgState { bgsCoits = newCoit : bgsCoits zBgState }
       ,zRes = ()
       ,zId = zId
      }
    rzipper =
      Zipper
      { zCursor = resetCursor
       ,zBgState = bgsReset zBgState
       ,zRes = def
       ,zId = zidReset zId
      }
    cmdItCoit' =
      DontReset $ case (bgsInits zBgState,acNonEmpty $ bgsIts zBgState) of
        ([],Nothing)
          -> CmdFinished $ ExZipper $ putRes newRes (zRes,finZipper)
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
    evolve zipper = return (mkZCat zipper,zipper)
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

-- | Apply the cap until finished. In the unPartialize function
-- Nothing means the cap was not reached so an final result does not
-- make sense. Use this to fast forward to a result and to stick to a
-- result.
mkMachine
  :: forall m w k .
  (Monad m,ZipperParams w,AShowW w)
  => ZipperId
  -> (GConf w -> Zipper w (ArrProc w m) -> Maybe k)
  -> [ArrProc w m]
  -> Arr (ArrProc w (FreeT (ItInit (ExZipper w) (ZItAssoc w)) m)) (GConf w) k
mkMachine zid getRes =
  handleLifetimes zid getRes
  . loopMooreCat -- feed the previous zipper to the next localizeConf
  . dimap fst (\z -> (z,z))
  . mkZCat' zid

-- | Compare the previous and next configurations to see of anything
-- changed. The problem with this approach is that the monad could
-- create a function to determine the lifespan. This way we can se the
-- lifespans more globally.
handleLifetimes
  :: forall m w k .
  (Monad m,ZipperParams w)
  => ZipperId
  -> (GConf w -> Zipper w (ArrProc w m) -> Maybe k)
  -> Arr
    (ArrProc' w (FreeT (Cmds' (ExZipper w) (ZItAssoc w)) m))
    (LConf w)
    (Zipper w (ArrProc w m))
  -> Arr (ArrProc w (FreeT (ItInit (ExZipper w) (ZItAssoc w)) m)) (GConf w) k
handleLifetimes zid getRes =
  evalResetsArr
  . hoistMealy (WriterArrow . rmap (\(nxt,(coepoch,z)) -> (coepoch,(nxt,z))))
  . mooreBatchC (Kleisli go)
  . hoistMoore
    (mempty,)
    (rmap (\(coepoch,(nxt,z)) -> (nxt,(coepoch,z))) . runWriterArrow)
  where
    -- (current config,(previous global epoch, previous zipper))
    -- ~>
    -- repeat config | result
    --
    -- This function transforms (within a Kleisli cat) the global
    -- configuration that is current to the iteration into a local
    -- configuration that has an updated epoch. The updated epoch is
    -- deduced based on the previous executions of the epoch.
    go :: (GConf w,(ZCoEpoch w,Zipper w (ArrProc w m)))
       -> FreeT
         (Cmds' (ExZipper w) (ZItAssoc w))
         m
         (Either (LConf w) (ZCoEpoch w,k))
    go (globConf,(coepoch,z0)) = case zLocalizeConf coepoch globConf z of
      ShouldReset -> wrapFree
        Cmds { cmdReset = DoReset $ go (globConf,(coepoch,resetZ))
              ,cmdItCoit = ShouldReset
             }
      DontReset conf' -> return $ ret conf'
      where
        z = z0 { zId = zidModTrail modTr $ zId z0 }
          where
            modTr [] = confTrPref globConf
            modTr curT =
              if curT == confTrPref globConf
              then curT else error "Changed trail!"
        resetZ =
          Zipper
          { zBgState = bgsReset $ zBgState z
           ,zRes = def :: ZPartialRes w
           ,zCursor = zCursor z
           ,zId = zidReset zid
          }
        ret conf' =
          maybe (Left conf') (\x -> Right (coepoch,x)) $ getRes globConf z

evalResetsArr
  :: forall conf m f r k s .
  Monad m
  => MealyArrow (WriterArrow s (Kleisli (FreeT (Cmds' r f) m))) conf k
  -> MealyArrow (WriterArrow s (Kleisli (FreeT (ItInit r f) m))) conf k
evalResetsArr (MealyArrow (WriterArrow (Kleisli c0))) =
  MealyArrow $ WriterArrow $ Kleisli $ \conf -> go $ c0 conf
  where
    go :: FreeT
         (Cmds' r f)
         m
         (s
         ,(MealyArrow (WriterArrow s (Kleisli (FreeT (Cmds' r f) m))) conf k,k))
       -> FreeT
         (ItInit r f)
         m
         (s
         ,(MealyArrow (WriterArrow s (Kleisli (FreeT (ItInit r f) m))) conf k
          ,k))
    go c = FreeT $ runFreeT c >>= \case
      Pure (s,(nxt,r)) -> return $ Pure (s,(evalResetsArr nxt,r))
      Free Cmds {cmdReset = DoReset rst,cmdItCoit = ShouldReset}
        -> runFreeT $ go rst
      Free Cmds {cmdItCoit = DontReset x} -> runFreeT $ wrapFree $ go <$> x

wrapFree :: Monad m => f (FreeT f m x) -> FreeT f m x
wrapFree = FreeT . return . Free

mkProc :: forall m w .
       (Monad m,ZipperParams w,AShowW w)
       => [ArrProc w m]
       -> ArrProc w m
mkProc = mkProcId $ zidDefault "no-id"

mkProcId
  :: forall m w .
  (Monad m,ZipperParams w,AShowW w)
  => ZipperId
  -> [ArrProc w m]
  -> ArrProc w m
mkProcId zid procs = evolution $ mkMachine zid evolutionControl procs
  where
    ZProcEvolution {..} = zprocEvolution
    evolution
      :: Arr
        (ArrProc w (FreeT (ItInit (ExZipper w) (ZItAssoc w)) m))
        (GConf w)
        (BndR w)
      -> ArrProc w m
    evolution (ArrProc p) =
      ArrProc
      $ fmap (\((coep,nxt),res) -> (coep,(nxt,res)))
      . evolutionStrategy (\v -> (mempty,arr $ const v)) -- nothing to add to the coepoch
      . fmap (\(coep,(nxt,res)) -> ((coep,evolution nxt),res))
      . p
    evolution _ = error "unreachable"

zSize :: (Foldable f,Foldable (ZItAssoc w)) => Zipper' w f p pr -> Int
zSize z =
  length (bgsIts $ zBgState z)
  + length (bgsInits $ zBgState z)
  + length (bgsCoits $ zBgState z)
  + length (zCursor z)

runMech :: ArrowFunctor c => MealyArrow c a b -> a -> ArrFunctor c b
runMech (MealyArrow c) ini = snd <$> toKleisli c ini

-- updateMap
--   :: MonadState s m
--   => s :>: MealyArrow (Kleisli m) a b
--   -> MealyArrow (Kleisli m) a b
--   -> MealyArrow (Kleisli m) a b
-- updateMap lens (MealyArrow (Kleisli m)) = MealyArrow $ Kleisli $ \a -> do
--   (nxt,r) <- m a
--   modify $ modL lens $ const nxt
--   return (nxt,r)

-- runLuMech :: MonadState s m => s :>: (MealyArrow (Kleisli m) a b) -> a -> m b
-- runLuMech lens ini = do
--   MealyArrow (Kleisli a) <- gets $ getL lens
--   snd <$> a ini
