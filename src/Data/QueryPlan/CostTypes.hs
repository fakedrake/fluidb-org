{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DerivingVia         #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
module Data.QueryPlan.CostTypes
  (GCCache(..)
  ,StarScore(..)
  ,MatCache(..)
  ,Frontiers(..)
  ,Cost(..)
  ,mkStarScore
  ,costAsInt
) where

import           Data.Foldable
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet        as HS
import           Data.NodeContainers
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Hashable
import           GHC.Generics


data Cost = Cost { costReads :: Int,costWrites :: Int }
  deriving (Show,Eq,Generic)
-- XXX: Here we are INCONSISTENT in assuming that reads and writes
-- cost the same.
instance Ord Cost where
  compare (Cost r w) (Cost r' w') = compare (r + w) (r' + w')
instance Num Cost where
  signum (Cost a b) = Cost (signum a) (signum b)
  abs (Cost a b) = Cost (abs a) (abs b)
  fromInteger x = Cost (fromInteger x) (fromInteger x)
  Cost a b + Cost a' b' = Cost (a + a') (b + b')
  Cost a b - Cost a' b' = Cost (a - a') (b - b')
  Cost a b * Cost a' b' = Cost (a * a') (b * b')

instance AShow Cost
instance Semigroup Cost where
  c1 <> c2 =
    Cost { costReads = costReads c1 + costReads c1
          ,costWrites = costWrites c1 + costWrites c2
         }
instance Monoid Cost where
  mempty = Cost  0 0
costAsInt :: Cost -> Int
costAsInt Cost{..} = costReads + costWrites * 10

data GCCache mop t n =
  GCCache { materializedMachines  :: RefMap n ()
          , isMaterializableCache :: MatCache mop t n
          , metaOpCache           :: RefMap n [(mop, Cost)]
          }
  deriving Generic
instance Default (GCCache mop t n)
data StarScore mop t n =
  StarScore   -- Total cost
  { starToDouble :: Double
    -- The metaops and their cost.
   ,starMetaOps  :: HS.HashSet (mop,Double)
  }
  deriving (Generic,Eq)
instance AShow mop => AShow (StarScore mop t n)
instance Hashables1 mop => Semigroup (StarScore mop t n) where
  ss <> ss' =
    StarScore
    { starToDouble = if length (starMetaOps ss) + length (starMetaOps ss')
        == length mops
        then starToDouble ss + starToDouble ss'
        else sum $ snd <$> toList mops
    , starMetaOps = mops
    }
    where
      mops = starMetaOps ss <> starMetaOps ss'

  {-# INLINE (<>) #-}
instance Default (StarScore mop t n)
instance Hashables1 mop => Monoid (StarScore mop t n) where mempty = def
instance Hashables1 mop => Ord (StarScore mop t n) where
  compare x y = compare (starToDouble x) (starToDouble y)

  {-# INLINE compare #-}
mkStarScore :: Hashables1 mop => mop -> Double -> StarScore mop t n
mkStarScore mop cost =
  StarScore { starToDouble = cost
            , starMetaOps = HS.singleton (mop, cost)
            }
data Frontiers mop t n = Frontiers {
  -- All the possible frontier/star pairs.
  frontierStars :: HM.HashMap (NodeSet n) (StarScore mop t n),
  -- Cache the best pair of star/frontier
  frontierStar  :: (NodeSet n,StarScore mop t n)}
  deriving Generic
instance Default (Frontiers mop t n)
instance AShow mop => AShow (Frontiers mop t n)
data MatCache mop t n =
  MatCache { matCacheDeps :: RefMap n [NodeRef n]
           , matCacheVals :: RefMap n (Frontiers mop t n)
           }
  deriving Generic
instance Default (MatCache mop t n)
instance AShow mop => AShow (MatCache mop t n)
instance Semigroup (MatCache mop t n) where
  m1 <> m2 =
    MatCache { matCacheDeps = matCacheDeps m1 <> matCacheDeps m2
             , matCacheVals = matCacheVals m1 <> matCacheVals m2
             }
instance Monoid (MatCache mop t n) where mempty = def
