{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DerivingVia         #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeFamilies        #-}
module Data.QueryPlan.CostTypes
  (GCCache(..)
  ,StarScore(..)
  ,MatCache(..)
  ,Frontiers(..)
  ,Cost(..)
  ,PlanCost(..)
  ,mkStarScore
  ,costAsInt
) where

import           Data.Foldable
import qualified Data.HashMap.Strict     as HM
import qualified Data.HashSet            as HS
import           Data.NodeContainers
import           Data.QueryPlan.Scalable
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Hashable
import           Data.Utils.Nat
import           GHC.Generics

data PlanCost n = PlanCost { pcPlan :: Maybe (NodeSet n),pcCost :: Cost }
  deriving (Show,Generic,Eq)
instance Scalable (PlanCost n) where
  scale sc pc = pc { pcCost = scale sc $ pcCost pc }
instance AShow (PlanCost n)
instance Zero (PlanCost n) where zero = mempty
instance Subtr (PlanCost n) where
  subtr (PlanCost _ a) (PlanCost _ b) = PlanCost Nothing $ subtr a b

instance Ord (PlanCost n) where
  compare c1 c2 = compare (pcCost c1) (pcCost c2)
  {-# INLINE compare #-}

instance Semigroup (PlanCost n) where
  c1 <> c2 =
    PlanCost
    { pcPlan = go (pcPlan c1) (pcPlan c1),pcCost = pcCost c1 <> pcCost c2 }
    where
      go Nothing a         = a
      go a Nothing         = a
      go (Just a) (Just b) = Just $ a <> b
instance Monoid (PlanCost n) where
  mempty = PlanCost { pcCost = mempty,pcPlan = mempty }

data Cost = Cost { costReads :: Int,costWrites :: Int }
  deriving (Show,Eq,Generic)

instance Zero Cost where
  zero = Cost 0 0
instance Subtr Cost where
  subtr (Cost a b) (Cost a' b') = Cost (a - a') (b - b')

-- XXX: Here we are INCONSISTENT in assuming that reads and writes
-- cost the same.
instance Ord Cost where
  compare a b = compare (costAsInt a) (costAsInt b)
  {-# INLINE compare #-}

instance AShow Cost where
  ashow' c = Sym $ show (costReads c) ++ "/" ++ show (costWrites c)
instance Semigroup Cost where
  c1 <> c2 =
    Cost { costReads = costReads c1 + costReads c2
          ,costWrites = costWrites c1 + costWrites c2
         }
instance Monoid Cost where
  mempty = Cost 0 0
costAsInt :: Cost -> Int
costAsInt Cost{..} = costReads + costWrites * 10
instance Scalable Cost where
  scale sc (Cost r w) = Cost (scale sc r) (scale sc w)
data GCCache mop t n =
  GCCache
  { materializedMachines  :: RefMap n ()
   ,isMaterializableCache :: MatCache mop t n
   ,metaOpCache           :: RefMap n [(mop,Cost)]
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
