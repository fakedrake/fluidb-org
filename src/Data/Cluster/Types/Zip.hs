{-# LANGUAGE RecordWildCards #-}
module Data.Cluster.Types.Zip
  ( Zip1(..)
  , Zip2(..)
  ) where

import           Data.Bifunctor
import           Data.Cluster.Types.Clusters
import           Data.Functor.Identity
import           Data.Utils.Compose
import           Data.Utils.EmptyF

class Functor f => Zip1 f where
  zip1 :: f x -> f y -> f (x,y)
  unzip1 :: f (x,y) -> (f x, f y)

instance Zip1 EmptyF where
  zip1 _ _ = EmptyF
  unzip1 _ = (EmptyF,EmptyF)
instance Zip1 Identity where
  zip1 (Identity a) (Identity b) = Identity (a,b)
  unzip1 (Identity (a,b)) = (Identity a,Identity b)

class Bifunctor c => Zip2 c where
  zip2 :: c a b -> c a' b' -> c (a,a') (b,b')
  unzip2 :: c (a,a') (b,b') -> (c a b, c a' b')

instance Zip2 (,) where
  zip2 (a,b) (a',b') = ((a,a'), (b,b'))
  unzip2 ((a,a'), (b,b')) = ((a,b),(a',b'))

instance (Functor f, Zip1 g, Zip1 f) => Zip1 (Compose f g) where
  zip1 (Compose x) (Compose y) = Compose $ uncurry zip1 <$> zip1 x y
  unzip1 (Compose f) = bimap Compose Compose $ unzip1 $ fmap unzip1 f

instance (Zip1 f, Zip1 g) => Zip2 (UnClust' f g) where
  zip2 c1 c2 = updateCHash UnClust {
    unClusterIn=zip1 (unClusterIn c1) (unClusterIn c2),
    unClusterPrimaryOut=zip1 (unClusterPrimaryOut c1) (unClusterPrimaryOut c2),
    unClusterSecondaryOut=zip1 (unClusterSecondaryOut c1) (unClusterSecondaryOut c2),
    unClusterT=zip1 (unClusterT c1) (unClusterT c2),
    unClusterHash=undefined
    }
  unzip2 UnClust{..} = (c1, c2) where
    c1 = updateCHash UnClust {
      unClusterIn=fst $ unzip1 unClusterIn,
      unClusterPrimaryOut=fst $ unzip1 unClusterPrimaryOut,
      unClusterSecondaryOut=fst $ unzip1 unClusterSecondaryOut,
      unClusterT=fst $ unzip1 unClusterT,
      unClusterHash=undefined
    }
    c2 = updateCHash UnClust {
      unClusterIn=snd $ unzip1 unClusterIn,
      unClusterPrimaryOut=snd $ unzip1  unClusterPrimaryOut,
      unClusterSecondaryOut=snd $ unzip1 unClusterSecondaryOut,
      unClusterT=snd $ unzip1 unClusterT,
      unClusterHash=undefined
    }


instance (Zip1 f, Zip1 g) => Zip2 (BinClust' f g) where
  zip2 c1 c2 = updateCHash BinClust {
    binClusterLeftIn=zip1 (binClusterLeftIn c1) (binClusterLeftIn c2),
    binClusterRightIn=zip1 (binClusterRightIn c1) (binClusterRightIn c2),
    binClusterOut=zip1 (binClusterOut c1) (binClusterOut c2),
    binClusterT=zip1 (binClusterT c1) (binClusterT c2),
    binClusterHash=undefined
    }
  unzip2 BinClust {..} = (c1,c2) where
    c1 = updateCHash BinClust {
      binClusterLeftIn=fst $ unzip1 binClusterLeftIn,
      binClusterRightIn=fst $ unzip1 binClusterRightIn,
      binClusterOut=fst $ unzip1 binClusterOut,
      binClusterT=fst $ unzip1 binClusterT,
      binClusterHash=undefined
    }
    c2 = updateCHash BinClust {
      binClusterLeftIn=snd $ unzip1 binClusterLeftIn,
      binClusterRightIn=snd $ unzip1 binClusterRightIn,
      binClusterOut=snd $ unzip1 binClusterOut,
      binClusterT=snd $ unzip1 binClusterT,
      binClusterHash=undefined
    }

instance (Zip1 f, Zip1 g) => Zip2 (JoinClust' f g) where
  zip2 c1 c2 = updateCHash JoinClust {
    joinBinCluster=zip2 (joinBinCluster c1) (joinBinCluster c2),
    joinClusterLeftAntijoin=
        zip1 (joinClusterLeftAntijoin c1) (joinClusterLeftAntijoin c2),
    joinClusterRightAntijoin=
        zip1 (joinClusterRightAntijoin c1) (joinClusterRightAntijoin c2),
    joinClusterLeftIntermediate=
        zip1 (joinClusterLeftIntermediate c1) (joinClusterLeftIntermediate c2),
    joinClusterRightIntermediate=
        zip1 (joinClusterRightIntermediate c1) (joinClusterRightIntermediate c2),
    joinClusterLeftSplit=
        zip1 (joinClusterLeftSplit c1) (joinClusterLeftSplit c2),
    joinClusterRightSplit=
        zip1 (joinClusterRightSplit c1) (joinClusterRightSplit c2),
    joinClusterHash=undefined
    }
  unzip2 JoinClust{..} = (c1,c2) where
    c1 = updateCHash JoinClust {
      joinBinCluster=fst $ unzip2 joinBinCluster,
      joinClusterLeftAntijoin=fst $ unzip1 joinClusterLeftAntijoin,
      joinClusterRightAntijoin=fst $ unzip1 joinClusterRightAntijoin,
      joinClusterLeftIntermediate=fst $ unzip1 joinClusterLeftIntermediate,
      joinClusterRightIntermediate=fst $ unzip1 joinClusterRightIntermediate,
      joinClusterLeftSplit=fst $ unzip1 joinClusterLeftSplit,
      joinClusterRightSplit=fst $ unzip1 joinClusterRightSplit,
      joinClusterHash=undefined
    }
    c2 = updateCHash JoinClust {
      joinBinCluster=snd $ unzip2 joinBinCluster,
      joinClusterLeftAntijoin=snd $ unzip1 joinClusterLeftAntijoin,
      joinClusterRightAntijoin=snd $ unzip1 joinClusterRightAntijoin,
      joinClusterLeftIntermediate=snd $ unzip1 joinClusterLeftIntermediate,
      joinClusterRightIntermediate=snd $ unzip1 joinClusterRightIntermediate,
      joinClusterLeftSplit=snd $ unzip1 joinClusterLeftSplit,
      joinClusterRightSplit=snd $ unzip1 joinClusterRightSplit,
      joinClusterHash=undefined
    }
instance Zip1 g => Zip2 (NClust' f g) where
  zip2 (NClust x) (NClust y) = NClust $ zip1 x y
  unzip2 = bimap NClust NClust . unzip1 . nClustNode
