{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeOperators #-}
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

module Control.Antisthenis.Multiply (MulTag) where

import Data.Proxy
import Control.Monad.Trans.Free
import Data.Bifunctor
import Control.Applicative
import Data.Utils.Monoid
import Data.Utils.AShow
import Data.Maybe
import GHC.Generics
import Data.Utils.Functors
import Data.Utils.Debug
import Data.Utils.Default
import Control.Monad.Identity
import Control.Antisthenis.AssocContainer
import Control.Antisthenis.Types


data MulTag p v

-- | A partial result should always be able to reverse a bound result
-- pushed to it. The absorbing elements therefore are counted instead
-- of counted. It is assumed that v is an abelian group.
data PartialResMul w =
  PartialResMul
  { prBnd :: Maybe (ZBnd w),prRes :: Maybe (Either (ZErr w) (ZRes w)) }
  deriving Generic

instance (AShow (ExtError p),AShow v)
  => AShow (PartialResMul (MulTag p v))
instance Default (PartialResMul w)

-- | Transform a partial result into a result that can be interfaced
-- with other operations.
prBndR :: (Num v,AShow (Mul v))
       => PartialResMul (MulTag p v)
       -> Maybe (BndR (MulTag p v))
prBndR pr = case (prBnd pr,prRes pr) of
  (Nothing,Nothing) -> Nothing
  (Just bnd,Nothing) -> Just $ BndBnd bnd
  (_,Just (Left e)) -> Just $ BndErr e
  (Just bnd,Just (Right res@(Mul zs _)))
    -> if zs > 0 then Just $ BndRes res else Just $ BndBnd $ res <> bnd
  (Nothing,Just (Right res)) -> Just $ BndRes res

prModBnd :: (ZBnd w,ZBnd w -> ZBnd w) -> PartialResMul w -> PartialResMul w
prModBnd (v,f) pr = pr { prBnd = Just $ maybe v f $ prBnd pr }
prModRes
  :: forall w . ZipperParams w => (ZRes w,ZRes w -> ZRes w) -> PartialResMul w -> PartialResMul w
prModRes (v,f) pr =
  pr { prRes = Just $ maybe (Right v) (fmap f) $ prRes pr }

prModBnd'
  :: (ZBnd (MulTag p v) -> ZBnd (MulTag p v))
  -> PartialResMul (MulTag p v)
  -> Maybe (PartialResMul (MulTag p v))
prModBnd' f pr = (\x -> pr { prBnd = Just $ f x }) <$> prBnd pr

prSetResErr :: ZErr (MulTag p v) -> PartialResMul (MulTag p v) -> PartialResMul (MulTag p v)
prSetResErr e pr@PartialResMul {prRes = Just (Right (Mul 0 _))} =
  pr { prRes = Just $ Left e }
prSetResErr _ pr@PartialResMul {prRes = Just (Right _)} = pr
prSetResErr e pr = pr { prRes = Just $ Left e }

instance BndRParams (MulTag p a) where
  type ZBnd (MulTag p a) = Mul a

  type ZRes (MulTag p a) = Mul a

  type ZErr (MulTag p a) = ExtError p

instance (ExtParams p,AShow a,Eq a,Num a,Integral a)
  => ZipperParams (MulTag p a) where
  type ZEpoch (MulTag p a) = ExtEpoch p
  type ZCoEpoch (MulTag p a) = ExtCoEpoch p
  type ZCap (MulTag p a) = Mul a
  type ZItAssoc (MulTag p a) =
    MulAssocList Maybe (Mul a)
  type ZPartialRes (MulTag p a) =
    PartialResMul (MulTag p a)
  zprocEvolution =
    ZProcEvolution
    { evolutionControl = mulSolution
     ,evolutionStrategy = mulStrategy
     ,evolutionEmptyErr = undefined
    }
  putRes newBnd (oldRes,newZipper) = newZipper <&> \() -> case newBnd of
    BndBnd x -> prModBnd (x,(* x)) oldRes
    BndRes x@(Mul 0 _) -> prModRes (x,(* x)) oldRes
    BndRes x -> oldRes { prRes = Just $ Right x }
    BndErr e -> prSetResErr e oldRes
  replaceRes oldBnd newBnd (oldRes,newZipper) = do
    lackingRes <- lackingResM
    return $ putRes newBnd (lackingRes,newZipper)
    where
      lackingResM = prModBnd' (`div` oldBnd) oldRes
  zLocalizeConf coepoch conf z =
    trace "localize multiply"
    $ extCombEpochs (Proxy :: Proxy p) coepoch (confEpoch conf)
    $ conf { confCap = newCap }
    where
      newCap = fromZ (prRes $ zRes z) $ case confCap conf of
        CapStruct _i -> undefined
        CapVal c -> case zRes z of
          PartialResMul {prBnd = Just bnd,prRes = Just (Right res)} -> CapVal
            $ c `div` (bnd * res)
          PartialResMul {prBnd = Nothing,prRes = Just (Right res)}
            -> CapVal $ c `div` res
          PartialResMul {prBnd = Just bnd,prRes = Nothing} -> CapVal $ c `div` bnd
          _ -> CapStruct 1
        x -> x
      fromZ r x = case r of
        Just (Left _) -> CapStruct 1
        Just (Right (Mul 0 _)) -> x
        Just (Right _is_zero) -> trace "Found zero" CapStruct (-1) -- HERE WE BLOCK ON ZERO
        _ -> x

-- | Keep zeroes separate so they are easily accessible and have an
-- obvious non-empty version.
data MulAssocList f v a =
  MulAssocList { malHead :: f (Either a (v,a)),malZ :: [a],malList :: [(v,a)] }
  deriving Generic
instance (AShow (f (Either a (v,a))),AShow v,AShowV a)
  => AShow (MulAssocList f v a)
instance Functor f => Functor (MulAssocList f v) where
  fmap f mal =
    MulAssocList
    { malHead = bimap f (fmap f) <$> malHead mal
     ,malZ = f <$> malZ mal
     ,malList = fmap2 f $ malList mal
    }
instance Foldable f => Foldable (MulAssocList f v) where
  foldr f i mal =
    foldr (f . either id snd) (foldr f (foldr2 f i $ malList mal) $ malZ mal)
    $ malHead mal
  null mal = null $ malHead mal

instance (Eq v,Num v) => AssocContainer (MulAssocList Maybe v) where
  type KeyAC (MulAssocList Maybe v) = v
  type NonEmptyAC (MulAssocList Maybe v) = MulAssocList Identity v
  acInsert k a0 mal = case malHead mal of
    Nothing -> MulAssocList
      { malHead = insHead,malZ = malZ mal,malList = malList mal }
    Just h@(Left _) -> case runIdentity insHead of
      Left a -> MulAssocList
        { malHead = Identity h,malZ = a : malZ mal,malList = malList mal }
      Right a -> MulAssocList
        { malHead = Identity h,malZ = malZ mal,malList = a : malList mal }
    Just (Right x) -> MulAssocList
      { malHead = insHead,malZ = malZ mal,malList = x : malList mal }
    where
      insHead = Identity $ if k == 0 then Left a0 else Right (k,a0)
  acUnlift mal =
    MulAssocList (Just $ runIdentity $ malHead mal) (malZ mal) (malList mal)
  acEmpty = MulAssocList Nothing [] []
  acNonEmpty mal =
    (\h -> MulAssocList (Identity h) (malZ mal) (malList mal)) <$> malHead mal

mulSolution
  :: forall v p x .
  (AShow v,Ord v,Num v)
  => Conf (MulTag p v)
  -> Zipper (MulTag p v) x
  -> Maybe (BndR (MulTag p v))
mulSolution conf z = zeroRes <|> ret
  where
    zeroRes = case prRes $ zRes z of
      Just (Right m@(Mul zs _)) -> if zs > 0 then Just $ BndRes m else Nothing
      _ -> Nothing
    ret = case (prRes $ zRes z,confCap conf) of
      (_,CapStruct i) -> if i < 0 then return $ BndErr undefined else resM
      (_,ForceResult) -> resM >>= \case
        BndBnd _bnd -> Nothing
        BndErr _e -> Nothing -- xxx: should check if zero is even possible.
        x -> Just x
      (_,CapVal cap) -> resM >>= \case
        BndBnd bnd -> if bnd <= cap then Nothing else Just $ BndBnd bnd
        x -> Just x
      where
        noInits = null $ bgsInits $ zBgState z
        resM :: Maybe (BndR (MulTag p v))
        resM = if noInits then prBndR $ zRes z else Nothing

mulStrategy
  :: (Num v,Monad m,AShow v)
  => r
  -> FreeT
    (ItInit (ExZipper (MulTag p v)) (ZItAssoc (MulTag p v)))
    m
    (r,BndR (MulTag p v))
  -> m (r,BndR (MulTag p v))
mulStrategy fin = recur
  where
    recur (FreeT m) = m >>= \case
      Pure a -> return a
      Free f -> case f of
        CmdItInit _it ini -> recur ini
        CmdIt it -> recur $ it malPop
        CmdInit ini -> recur ini
        CmdFinished (ExZipper z) -> return
          (fin,fromMaybe (BndErr undefined) $ prBndR $ zRes z)

malPop :: Num v => MulAssocList Identity v x -> (v,x,MulAssocList Maybe v x)
malPop MulAssocList {malHead = Identity h,..} = case h of
  Left x -> (fromInteger 0,x,mkMal malZ malList)
  Right (v,x) -> (v,x,mkMal malZ malList)
mkMal :: [a] -> [(v,a)] -> MulAssocList Maybe v a
mkMal (a:as) list =
  MulAssocList { malHead = Just (Left a),malZ = as,malList = list }
mkMal [] (a:as) =
  MulAssocList { malHead = Just (Right a),malZ = [],malList = as }
mkMal [] [] = MulAssocList { malHead = Nothing,malZ = [],malList = [] }

#if 0
assert :: MonadFail m => Bool -> m ()
assert True = return ()
assert False = fail "assertion failed"

-- | TESTING
mulTest
  :: IO (BndR (MulTag TestParams Integer))
mulTest =
  fmap (fst . fst)
  $ (`runReaderT` mempty)
  $ (`runFixStateT` def)
  $ runWriterT
  $ do
    putMech 1 $ incrTill "1" ((+ 1),id) 3
    putMech 2 $ incrTill "B" ((+ 1),id) 3
    putMech 3 $ zeroAfter 4
    let insTrail k tr =
          if k `IS.member` tr
          then Left $ ErrCycle k tr else Right $ IS.insert k tr
    let getMech i =
          withTrail (insTrail i) $ getUpdMech (BndErr $ ErrMissing i) i
    res <- runMech (mkProc $ getMech <$> [1,2,3]) def
    lift3 $ putStrLn $ "Result is: " ++ ashow res
    assert $ case res of
      BndRes _ -> True
      _ -> False
    return res
#endif
