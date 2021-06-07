{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Data.QueryPlan.Materializable
  (isMaterializable) where

import Control.Antisthenis.Convert
import Control.Arrow
import qualified Control.Category as C
import Data.QueryPlan.ProcTrail
import Data.QueryPlan.MetaOp
import Data.Utils.Functors
import Control.Antisthenis.Types
import Data.Utils.AShow
import Control.Antisthenis.ATL.Transformers.Mealy
import Data.Maybe
import Data.Utils.Functors
import Control.Antisthenis.Zipper
import Control.Monad.State
import Control.Monad.Except
import Control.Monad.Writer
import Data.Utils.Default
import Control.Monad.Reader
import Data.QueryPlan.Nodes
import Control.Monad.Identity
import Control.Antisthenis.Bool
import Control.Antisthenis.Types
import Data.List.NonEmpty as NEL
import Data.QueryPlan.Types
import Data.NodeContainers

isMaterializable :: Monad m =>  NodeRef n -> PlanT t n m Bool
isMaterializable ref = do
  states <- gets $ fmap isMat . nodeStates . NEL.head . epochs
  ((res,_coepoch),_trail) <- planQuickRun
    $ (`runStateT` def)
    $ runWriterT
    $ runMech (getOrMakeMech ref)
    $ Conf
    { confCap = ForceResult
     ,confEpoch = states
     ,confTrPref = "topLevel:" ++ ashow ref
    }
  case res of
    BndRes GBool {..} -> return $ unExists gbTrue
    BndBnd _bnd -> throwPlan $ "We forced the result but got a partial result."
    BndErr e -> throwPlan $ "antisthenis error: " ++ ashow e

type OrTag n = BoolTag Or (PlanParams n)
type AndTag n = BoolTag And (PlanParams n)
makeIsMatableProc
  :: forall t n .
  NodeRef n
  -> [[NodeProc t n (BoolTag Or (PlanParams n) )]]
  -> NodeProc t n (BoolTag Or (PlanParams n))
makeIsMatableProc ref deps =
  procOr
  $ lowerNodeProc andToOrConv . procAnd . fmap (liftNodeProc $ orToAndConv)
  <$> deps
  where
    procAnd :: [NodeProc0 t n (OrTag n) (AndTag n)]
            -> NodeProc0 t n (OrTag n) (AndTag n)
    procAnd ns = arr (\conf -> conf { confTrPref = mid }) >>> mkProcId mid ns
      where
        mid = "min:" ++ ashow ref
    procOr :: [NodeProc t n (OrTag n)] -> NodeProc t n (OrTag n)
    procOr ns = arr (\conf -> conf { confTrPref = mid }) >>> mkProcId mid ns
      where
        mid = ("sum:" ++ ashow ref)

getOrMakeMech
  :: NodeRef n -> NodeProc t n (BoolTag Or (PlanParams n))
getOrMakeMech ref =
  squashMealy
  $ lift2
  $ gets
  $ fromMaybe (mkNewMech ref) . refLU ref . matableMechMap

-- | Build AND INSERT a new mech in the mech directory.
mkNewMech :: NodeRef n -> NodeProc t n (BoolTag Or (PlanParams n))
mkNewMech ref = squashMealy $ do
  mops <- lift2 $ findCostedMetaOps ref
  -- Should never see the same val twice.
  let mechs =
        [[getOrMakeMech n | n <- toNodeList $ metaOpIn mop]
        | (mop,_cost) <- mops]
  let ret =
        withTrail (ErrCycle (runNodeRef ref) . runNodeSet) ref
        $ mkEpoch _zero ref >>> C.id ||| makeIsMatableProc ref mechs
  lift2 $ modify $ \gcs
    -> gcs { matableMechMap = refInsert ref ret $ matableMechMap gcs }
  return ret


-- | Run PlanT in the Identity monad.
planQuickRun :: Monad m => PlanT t n Identity a -> PlanT t n m a
planQuickRun m = do
  st0 <- get
  conf <- ask
  case runIdentity $ runExceptT $ (`runReaderT` conf) $ (`runStateT` st0) m of
    Left e -> throwError e
    Right (a,st) -> put st >> return a
