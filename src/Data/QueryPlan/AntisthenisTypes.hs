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
  ,MatTag
  ,MatTag0
  ,IsPlanParams
  ,IsPlanCostParams
  ,MatParams
  ,CostParams) where

import           Control.Antisthenis.Bool
import           Control.Antisthenis.Lens
import           Control.Antisthenis.Minimum
import           Control.Antisthenis.Sum
import           Control.Antisthenis.Types
import qualified Data.IntSet                 as IS
import           Data.NodeContainers
import           Data.Pointed
import           Data.Proxy
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.HistBnd
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Nat
import           GHC.Generics

-- | Tags to disambiguate the properties of historical and cost processes.
data HistTag
data CostTag
data MatTag0 op
type MatTag = MatTag0 Or
type MatParams n = BoolTag Or (PlanParams MatTag n)
type CostParams tag n = SumTag (PlanParams tag n)
data PlanParams tag n
type family MetaTag t :: *
type instance MetaTag (SumTag p) = p
type instance MetaTag (MinTag p) = p
type instance MetaTag (BoolTag op p) = p

instance (BndRParams w,ZCap w ~ ExtCap (PlanParams (MatTag0 op) n))
  => ExtParams w (PlanParams (MatTag0 op) n) where
  type MechVal (PlanParams (MatTag0 op) n) = BoolV op
  type ExtError (PlanParams (MatTag0 op) n) =
    IndexErr (NodeRef n)
  type ExtEpoch (PlanParams (MatTag0 op) n) = PlanEpoch n
  type ExtCoEpoch (PlanParams (MatTag0 op) n) = PlanCoEpoch n
  type ExtCap (PlanParams (MatTag0 op) n) = BoolCap op
  extExceedsCap Proxy cap bnd = exceedsCap @w Proxy cap bnd
  extCombEpochs _ = planCombEpochs

-- | We use the general w parameter to cover both min and sum for
-- params.
instance ZBnd w ~ ExtCap (PlanParams CostTag n)
  => ExtParams w (PlanParams CostTag n) where
  type MechVal (PlanParams CostTag n) = PlanCost n
  type ExtError (PlanParams CostTag n) =
    IndexErr (NodeRef n)
  type ExtEpoch (PlanParams CostTag n) = PlanEpoch n
  type ExtCoEpoch (PlanParams CostTag n) = PlanCoEpoch n
  type ExtCap (PlanParams CostTag n) =
    Min (MechVal (PlanParams CostTag n))
  extExceedsCap Proxy cap bnd = case compare bnd cap of
    LT -> BndWithinCap bnd
    EQ -> EqualCap
    GT -> BndExceeds
  extCombEpochs _ = planCombEpochs

maxMatTrail :: Int
maxMatTrail = 4
instance ZBnd w ~ Min (MechVal (PlanParams HistTag n))
  => ExtParams w (PlanParams HistTag n) where
  type MechVal (PlanParams HistTag n) = HistVal Cost
  type ExtError (PlanParams HistTag n) =
    IndexErr (NodeRef n)
  type ExtEpoch (PlanParams HistTag n) = PlanEpoch n
  type ExtCoEpoch (PlanParams HistTag n) = PlanCoEpoch n
  type ExtCap (PlanParams HistTag n) = HistCap Cost
  extExceedsCap _ HistCap {..} (Min (Just bnd)) = case compBndCap of
    (GT,_,_)   -> BndExceeds
    (_,GT,_)   -> BndExceeds
    (_,_,GT)   -> BndExceeds
    (EQ,EQ,EQ) -> EqualCap
    (_,_,_)    -> BndWithinCap newCap
    where
      newCap =
        HistCap
        { hcNonCompTolerance = bndNonComp
         ,hcValCap = bndVal
         ,hcMatsEncountered = hcMatsEncountered
        }
      -- XXX: gets stuck getting an uncertain bound and the cap not
      -- budging.
      compBndCap =
        (compare bndVal hcValCap   -- reverse
        ,compare (maxMatTrail - hcMatsEncountered) bndTrailSize
        ,compare bndNonComp hcNonCompTolerance)
      bndNonComp = hvNonComp bnd
      bndVal = point $ hvVal bnd
      bndTrailSize = maybe 0 fst $ IS.maxView $ hvMaxMatTrail bnd
  extExceedsCap _ _ (Min Nothing) = BndExceeds
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
  { peCoPred :: CoPredicates n
   ,peParams :: RefMap n Bool
  }
  deriving Generic
data PlanCoEpoch n =
  PlanCoEpoch
  { pcePred   :: Predicates n
   ,pceParams :: RefMap n Bool
  }
  deriving (Show,Eq,Generic)
instance AShow (PlanCoEpoch n) where
  ashow' _ = Sym "<PlanCoEpoch>"
instance AShow (PlanEpoch n) where
  ashow' PlanEpoch {..} = ashow' peCoPred

instance Default (PlanEpoch n)
instance Monoid (PlanCoEpoch n) where
  mempty = PlanCoEpoch mempty mempty
instance Semigroup (PlanCoEpoch n) where
  PlanCoEpoch a b <> PlanCoEpoch a' b' =
    PlanCoEpoch (a <> a') (b <> b')
