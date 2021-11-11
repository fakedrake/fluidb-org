{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE UndecidableInstances #-}
module Data.QueryPlan.AntisthenisTypes
  (HistTag
  ,CostTag
  ,MetaTag
  ,PlanParams
  ,Predicates(..)
  ,CoPredicates
  ,PlanEpoch(..)
  ,PlanCoEpoch(..)
  ,IsPlanParams
  ,IsPlanCostParams
  ,MatParams
  ,CostParams) where

import           Control.Antisthenis.Bool
import           Control.Antisthenis.Lens
import           Control.Antisthenis.Minimum
import           Control.Antisthenis.Sum
import           Control.Antisthenis.Types
import           Data.NodeContainers
import           Data.Proxy
import           Data.QueryPlan.Cert
import           Data.QueryPlan.Comp
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.HistBnd
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Nat
import           GHC.Generics

-- | Tags to disambiguate the properties of historical and cost processes.
data HistTag
data CostTag
data MatTag
type MatParams n = BoolTag Or (PlanParams MatTag n)
type CostParams tag n = SumTag (PlanParams tag n)
data PlanParams tag n
type family MetaTag t :: *
type instance MetaTag (SumTag p) = p
type instance MetaTag (MinTag p) = p
type instance MetaTag (BoolTag op p) = p

instance ExtParams (MatParams n) (PlanParams MatTag n) where
  type MechVal (PlanParams MatTag n) = BoolV Or
  type ExtError (PlanParams MatTag n) =
    IndexErr (NodeRef n)
  type ExtEpoch (PlanParams MatTag n) = PlanEpoch n
  type ExtCoEpoch (PlanParams MatTag n) = PlanCoEpoch n
  type ExtCap (PlanParams MatTag n) =
    ZCap (MatParams n)
  extExceedsCap Proxy cap bnd =
    exceedsCap @(MatParams n) Proxy cap bnd
  extCombEpochs _ = planCombEpochs

instance ZBnd w ~ ExtCap (PlanParams CostTag n)
  => ExtParams w (PlanParams CostTag n) where
  type MechVal (PlanParams CostTag n) = PlanCost n
  type ExtError (PlanParams CostTag n) =
    IndexErr (NodeRef n)
  type ExtEpoch (PlanParams CostTag n) = PlanEpoch n
  type ExtCoEpoch (PlanParams CostTag n) = PlanCoEpoch n
  type ExtCap (PlanParams CostTag n) =
    Min (MechVal (PlanParams CostTag n))
  extExceedsCap Proxy cap bnd = cap < bnd
  extCombEpochs _ = planCombEpochs

maxMatTrail :: Int
maxMatTrail = 4
instance ZBnd w ~ Min (MechVal (PlanParams HistTag n))
  => ExtParams w (PlanParams HistTag n) where
  type MechVal (PlanParams HistTag n) =
    Cert (Comp Cost)
  type ExtError (PlanParams HistTag n) =
    IndexErr (NodeRef n)
  type ExtEpoch (PlanParams HistTag n) = PlanEpoch n
  type ExtCoEpoch (PlanParams HistTag n) = PlanCoEpoch n
  type ExtCap (PlanParams HistTag n) = HistCap Cost
  extExceedsCap _ HistCap {..} (Min (Just bnd)) =
    maybe False (cValue (cData bnd) >) (getMin hcValCap)
    || cTrailSize bnd > (maxMatTrail - hcMatsEncountered)
    || cProbNonComp (cData bnd) > hcNonCompTolerance
  extExceedsCap _ _ (Min Nothing) = False
  extCombEpochs _ = planCombEpochs

--  | All the constraints required to run both min and sum
type IsPlanCostParams w n =
  (IsPlanParams w n
  ,Subtr (MechVal (MetaTag w))
  ,Ord (MechVal (MetaTag w))
   -- For updating the cap
  ,Zero (MechVal (MetaTag w))
  ,Zero (ExtCap (MetaTag w))
  ,HasLens (ExtCap (MetaTag w)) (Min (MechVal (MetaTag w)))
  ,ExtParams (MinTag (MetaTag w)) (MetaTag w)
  ,ExtParams (SumTag (MetaTag w)) (MetaTag w))
type IsPlanParams w n =
  (ExtError (MetaTag w) ~ IndexErr
     (NodeRef n)
  ,ExtEpoch (MetaTag w) ~ PlanEpoch n
  ,ExtCoEpoch (MetaTag w) ~ PlanCoEpoch n

  ,ZEpoch w ~ PlanEpoch n
  ,ZCoEpoch w ~ PlanCoEpoch n
  ,ZErr w ~ IndexErr (NodeRef n)
  ,AShowV (MechVal (MetaTag w))
  ,Semigroup (MechVal (MetaTag w))
  ,AShowV (ExtCap (MetaTag w)))

-- | When the coepoch is older than the epoch we must reset and get
-- a fresh value for the process. Otherwise the progress made so far
-- towards a value is valid and we should continue from there.
--
-- XXX: The cap may have changed though
planCombEpochs :: PlanCoEpoch n -> PlanEpoch n -> a -> MayReset a
planCombEpochs coepoch epoch a =
  if paramsMatch && predicatesMatch then DontReset a else ShouldReset
  where
    predicatesMatch =
      nsNull (peCoPred epoch)
      || nsDisjoint (pNonComputables $ pcePred coepoch) (peCoPred epoch)
    paramsMatch =
      and
      $ refIntersectionWithKey
        (const (==))
        (pceParams coepoch)
        (peParams epoch)

-- Node procs
-- | NodeProc t n ()
newtype Predicates n = Predicates { pNonComputables :: NodeSet n }
  deriving (Eq,Show,Generic)
instance AShow (Predicates n)
instance Semigroup (Predicates n) where
  p <> p' =
    Predicates { pNonComputables = pNonComputables p <> pNonComputables p' }
instance Monoid (Predicates n) where
  mempty = Predicates mempty
type CoPredicates n = NodeSet n
data PlanEpoch n =
  PlanEpoch
  { peCoPred       :: CoPredicates n
   ,peParams       :: RefMap n Bool
   ,peMaterialized :: Integer
  }
  deriving Generic
data PlanCoEpoch n =
  PlanCoEpoch
  { pcePred       :: Predicates n
   ,pceParams     :: RefMap n Bool
   ,pceMaterlized :: Integer
  }
  deriving (Show,Eq,Generic)
instance AShow (PlanCoEpoch n)
  -- ashow' PlanCoEpoch {..} = ashow' pcePred
instance AShow (PlanEpoch n) where
  ashow' PlanEpoch {..} = ashow' peCoPred

instance Default (PlanEpoch n)
instance Monoid (PlanCoEpoch n) where
  mempty = PlanCoEpoch mempty mempty 0
instance Semigroup (PlanCoEpoch n) where
  PlanCoEpoch a b d <> PlanCoEpoch a' b' d' =
    PlanCoEpoch (a <> a') (b <> b') (max d d')
