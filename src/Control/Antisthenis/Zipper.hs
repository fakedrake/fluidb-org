{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Arrows #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveFunctor #-}

module Control.Antisthenis.Zipper (zSize,mkZCat,mkMachine,mkProc,runMech) where

import Control.Monad.Trans.Free
import Data.Utils.EmptyF
import Control.Monad.Identity
import Data.Utils.Debug
import Data.Proxy
import Data.Maybe
import Data.Bifunctor
import Data.Utils.Default
import Data.Utils.Unsafe
import Control.Monad.State
import Data.Foldable
import Data.Profunctor
import Control.Antisthenis.ATL.Transformers.Mealy
import Control.Antisthenis.ATL.Transformers.Moore
import Control.Antisthenis.AssocContainer
import Control.Arrow hiding (first)
import Control.Antisthenis.Types


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
    (Kleisli (FreeT (Cmds w) m))
    (LConf w)
    (Zipper w (ArrProc w m))


runArrProc :: LConf w -> ArrProc w m -> m (ArrProc w m,BndR w)
runArrProc conf (MealyArrow (Kleisli p)) = p conf

-- | Build an evolving zipper. Provided a local conf this zipper may
-- rotated towards any (allowed) direction. After each rotation the
-- allowed directions are encapsulated in the return type. It is in
-- theory allowed that more than one steps are required (see Free
-- monad) but this one requests the local conf which can always be
-- resolved in one step.
mkZCat'
  :: forall w m .
  (ZipperParams w,Monad m)
  => [ArrProc w m]
  -> MooreCat (Kleisli (FreeT (Cmds w) m)) (LConf w) (Zipper w (ArrProc w m))
mkZCat' [] = error "mul needs at least one value."
mkZCat' (a:as) =
  mkMooreCat
  (trace ("Zipper: " ++ show (length $ bgsInits $ zBgState zipper)) zipper)
  $ mkZCat zipper
  where
    zipper =
      Zipper { zCursor = Identity (Nothing,a,a),zBgState = mkGgState as,zRes = def }

mkZCat
  :: (ZipperParams w,Monad m)
  => Zipper w (ArrProc w m)
  -> MealyArrow (Kleisli (FreeT (Cmds w) m)) (LConf w) (Zipper w (ArrProc w m))
mkZCat prevz = MealyArrow $ Kleisli $ \lconf -> FreeT $ do
  let (_bnd,inip,p) = runIdentity $ zCursor prevz
  (p',val) <- runArrProc lconf p
  runFreeT $ case val of
    BndBnd bnd -> pushIt (bnd,inip,p') (zRes prevz) $ zBgState prevz
    BndRes res -> pushCoit (Right res,p') (zRes prevz) $ zBgState prevz
    BndErr err -> pushCoit (Left err,p') (zRes prevz) $ zBgState prevz



type New v = v
type Old v = v
-- | This is almost a Mealy (Kleisly) arrow. From a gutted zipper (the
-- previous step was setting the cursor on an it) there are a couple
-- of ways to put the zipper together (either pull it or ini or reset
-- etc). Create the commands for that.
pushIt
  :: forall w m p .
  (Monad m,p ~ ArrProc w m,ZipperParams w)
  => New (ZBnd w,InitProc p,p)
  -> Old (ZPartialRes w)
  -> Old (ZipState w p)
  -> FreeT (Cmds w) m (ZCat w m,Zipper w p)
pushIt (bnd,inip,itp) oldRes bgs =
  wrap
  $ Cmds { cmdItCoit = cmdItCoit'
          ,cmdReset = DoReset $ return (mkZCat rzipper,rzipper)
         }
  where
    rzipper =
      Zipper
      { zCursor = Identity (Nothing,inip,inip)
       ,zBgState = bgsReset bgs
       ,zRes = def
      }
    cmdItCoit' = DontReset $ case bgsInits bgs of
      [] -> CmdIt itEvolve
      ini:inis -> CmdItInit itEvolve $ iniEvolve ini inis
    iniEvolve ini inis = return (mkZCat zipper,zipper)
      where
        zipper :: Zipper w p
        zipper =
          putRes
            (BndBnd bnd)
            (oldRes
            ,Zipper
             { zCursor = Identity (Nothing,ini,ini)
              ,zBgState = bgs
                 { bgsIts = acUnlift $ acInsert bnd (inip,itp) $ bgsIts bgs
                  ,bgsInits = inis
                 }
              ,zRes = ()
             })
    -- Evolve by poping an it
    itEvolve pop = return (mkZCat zipper,zipper)
      where
        zipper =
          Zipper { zCursor = Identity (Just bnd',inip',itp')
                  ,zBgState = bgs { bgsIts = its' }
                  ,zRes = oldRes
                 }
        (bnd' :: ZBnd w,(inip',itp'),its') =
          pop $ acInsert bnd (inip,itp) $ (bgsIts bgs)

pushCoit
  :: forall w m p .
  (Monad m,p ~ ArrProc w m,ZipperParams w)
  => New (Either (ZErr w) (ZRes w),CoitProc p)
  -> ZPartialRes w
  -> ZipState w p
  -> FreeT (Cmds w) m (ZCat w m,Zipper w p)
pushCoit newCoit oldRes bgs =
  wrap
  $ Cmds { cmdItCoit = cmdItCoit'
          ,cmdReset = DoReset $ return (mkZCat rzipper,rzipper)
         }
  where
    newRes = either BndErr BndRes $ fst newCoit
    resetCursor = Identity (Nothing,snd newCoit,snd newCoit)
    finZipper =
      Zipper
      { zCursor = EmptyF
       ,zBgState = bgs { bgsCoits = newCoit : bgsCoits bgs }
       ,zRes = ()
      }
    rzipper =
      Zipper { zCursor = resetCursor,zBgState = bgsReset bgs,zRes = def }
    cmdItCoit' = DontReset $ case (bgsInits bgs,acNonEmpty $ bgsIts bgs) of
      ([],Nothing) -> CmdFinished $ ExZipper $ putRes newRes (oldRes,void finZipper)
      (ini:inis,Nothing) -> CmdInit $ evolve $ mkIniZ ini inis
      ([],Just nonempty) -> CmdIt $ \pop -> evolve
        $ let (k,(ini,it),its) = pop nonempty
        in fromJustErr $ mkItZ (k,ini,it) its
      (ini:inis,Just nonempty) -> CmdItInit
        (\pop -> evolve
         $ let (k,(ini',it),its) = pop nonempty
         in fromJustErr $ mkItZ (k,ini',it) its)
        (evolve $ mkIniZ ini inis)
    evolve zipper = return (mkZCat zipper,zipper)
    mkIniBgs inis = bgs { bgsCoits = newCoit : bgsCoits bgs,bgsInits = inis }
    mkIniZ ini inis =
      putRes
        newRes
        (oldRes
        ,Zipper { zCursor = Identity (Nothing,ini,ini)
                 ,zBgState = mkIniBgs inis
                 ,zRes = ()
                })
    mkItBgs its = bgs { bgsCoits = newCoit : bgsCoits bgs,bgsIts = its }
    mkItZ (bnd,ini,it) its =
      replaceRes
        bnd
        newRes
        (oldRes
        ,Zipper { zCursor = Identity (Just bnd,ini,it)
                 ,zBgState = mkItBgs its
                 ,zRes = ()
                })

-- | Apply the cap until finished. In the unPartialize function
-- Nothing means the cap was not reached so an final result does not
-- make sense. Use this to fast forward to a result and to stick to a
-- result.
mkMachine
  :: forall m w k .
  (ZipperMonad w m,Semigroup (ZRes w),Ord (ZBnd w),ZipperParams w,AShowW w)
  => (GConf w -> Zipper w (ArrProc w m) -> Maybe k)
  -> [ArrProc w m]
  -> MealyArrow
    (Kleisli (FreeT (ItInit (ExZipper w) (ZItAssoc w)) m))
    (GConf w)
    k
mkMachine getRes =
  handleLifetimes getRes
  . loopMooreCat -- feed the previous zipper to the next localizeConf
  . dimap
    (uncurry localizeConf)
    (\z -> trace ("mkMachine: " ++ ashow (ashowZ' z,zSize z)) (z,z))
  . mkZCat'

-- | Compare the previous and next configurations to see of anything
-- changed. The problem with this approach is that the monad could
-- create a function to determine the lifespan. This way we can se the
-- lifespans more globally.
handleLifetimes
  :: forall m w k .
  (Monad m,Semigroup (ZRes w),Ord (ZBnd w),ZipperParams w,ZipperMonad w m)
  => (GConf w -> Zipper w (ArrProc w m) -> Maybe k)
  -> MooreCat
    (Kleisli (FreeT (Cmds' (ExZipper w) (ZItAssoc w)) m))
    (LConf w)
    (Zipper w (ArrProc w m))
  -> MealyArrow
    (Kleisli (FreeT (ItInit (ExZipper w) (ZItAssoc w)) m))
    (GConf w)
    k
handleLifetimes getRes =
  evalResetsArr
  . mooreBatchC (Kleisli go) -- compare prev and current epoch to mark
                             -- for result
  . mooreFoldInputs (\a b -> Just $ fromMaybe (confEpoch a) b) Nothing -- make the previous input available
  where
    -- (current config,(previous config, previous zipper))
    -- ~>
    -- repeat config | result
    go :: (GConf w,(Maybe (ZEpoch w),Zipper w (ArrProc w m)))
       -> FreeT (Cmds' (ExZipper w) (ZItAssoc w)) m (Either (LConf w) k)
    go (conf,(prevEpochM,z)) = case prevEpochM of
      Just prevEpoch -> do
        shouldReset <- lift
          $ zCmpEpoch (Proxy :: Proxy w) (confEpoch conf) prevEpoch
        if shouldReset then wrap
          Cmds { cmdReset = DoReset $ go (conf,(Nothing,resetZ))
                ,cmdItCoit = ShouldReset
               } else return ret
      Nothing -> return ret
      where
        resetZ =
          Zipper
          { zBgState = bgsReset $ zBgState z,zRes = def,zCursor = zCursor z }
        ret = maybe (Left conf) Right $ getRes conf z

evalResetsArr
  :: forall conf m f r k .
  Monad m
  => MealyArrow (Kleisli (FreeT (Cmds' r f) m)) conf k
  -> MealyArrow (Kleisli (FreeT (ItInit r f) m)) conf k
evalResetsArr (MealyArrow (Kleisli c0)) = MealyArrow $ Kleisli $ \conf -> go
  (c0 conf)
  where
    go :: FreeT
         (Cmds' r f)
         m
         (MealyArrow (Kleisli (FreeT (Cmds' r f) m)) conf k,k)
       -> FreeT
         (ItInit r f)
         m
         (MealyArrow (Kleisli (FreeT (ItInit r f) m)) conf k,k)
    go c = FreeT $ runFreeT c >>= \case
      Pure (nxt,r) -> return $ Pure (evalResetsArr nxt,r)
      Free Cmds {cmdReset = DoReset rst,cmdItCoit = ShouldReset}
        -> runFreeT $ go rst
      Free Cmds {cmdItCoit = DontReset x} -> runFreeT $ wrap $ go <$> x


mkProc
  :: forall m w .
  (Semigroup (ZRes w)
  ,Monad m
  ,Ord (ZBnd w)
  ,ZipperParams w
  ,ZipperMonad w m
  ,AShowW w)
  => [ArrProc w m]
  -> ArrProc w m
mkProc procs = evolution $ mkMachine evolutionControl procs
  where
    ZProcEvolution {..} = zprocEvolution
    evolution
      :: MealyArrow
        (Kleisli (FreeT (ItInit (ExZipper w) (ZItAssoc w)) m))
        (GConf w)
        (BndR w)
      -> ArrProc w m
    evolution p =
      MealyArrow
      $ Kleisli
      $ fmap (evolutionStrategy (evolution p) . fmap (first evolution))
      $ runKleisli
      $ runMealyArrow p

zSize :: (Foldable f,Foldable (ZItAssoc w)) => Zipper' w f p pr -> Int
zSize z =
  length (bgsIts $ zBgState z)
  + length (bgsInits $ zBgState z)
  + length (bgsCoits $ zBgState z)
  + length (zCursor z)

runMech :: Functor m => MealyArrow (Kleisli m) a b -> a -> m b
runMech (MealyArrow (Kleisli a)) ini = snd <$> a ini


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
