{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs            #-}
{-# LANGUAGE LambdaCase       #-}
{-# LANGUAGE TupleSections    #-}
module Data.QueryPlan.Transitions
  (getTransitions
  ,transitionCost
  ,traverseTransition
  ,trigger
  ,revTrigger
  ,putTrigger
  ,putRevTrigger
  ,putTransition'
  ,totalTransitionCost
  ,putTransition
  ,putDelNode
  ,mkTriggerUnsafe) where

import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Bipartite
import qualified Data.List.NonEmpty   as NEL
import           Data.NodeContainers
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Types
import           Data.Utils.Default
import           Data.Utils.MTL
import           Data.Utils.Ranges
import           Text.Printf

-- | Make a transition for triggering t. This does not check
-- materialization of input/output.
mkTriggerUnsafe :: Monad  m =>
                  ([NodeRef n] -> NodeRef t -> [NodeRef n] -> Transition t n)
                -> ([IsReversible], [IsReversible])
                -> NodeRef t
                -> PlanT t n m (Transition t n)
mkTriggerUnsafe mkTrig (inf,outf) tref = do
  net <- asks propNet
  let getLinksFrom side dir =
        fmap toNodeList
        $ (>>= maybe (throwError $ NonExistentNode $ Left tref) return)
        $ lift
        $ evalStateT
          (getNodeLinksT
             NodeLinksFilter
             { nlfNode = tref,nlfSide = return side,nlfIsRev = dir })
          def { gbPropNet = net }
  inps <- getLinksFrom Inp inf
  outs <- getLinksFrom Out outf
  return $ mkTrig inps tref outs

putDelNode :: MonadHaltD m => NodeRef n -> PlanT t n m ()
putDelNode ref = do
  interm <- isIntermediate ref
  putTransition' (if interm then "(interm)" else "(not interm)") $ DelNode ref

putTransition :: MonadHaltD m => Transition t n -> PlanT t n m ()
putTransition = putTransition' ""
putTransition' :: MonadHaltD m =>
                String -> Transition t n -> PlanT t n m ()
putTransition' msg tr = do
  trM $ printf "Applying transition%s: %s" msg (show tr)
  gcs <- get
  let epoch = NEL.head $ epochs gcs
  let epoch' = epoch{transitions=tr:transitions epoch}
  put gcs{epochs=epoch' NEL.:| NEL.tail (epochs gcs)}
putRevTrigger :: MonadHaltD m => NodeRef t -> PlanT t n m ()
putRevTrigger = putTransition <=< revTrigger
revTrigger :: Monad m => NodeRef t -> PlanT t n m (Transition t n)
revTrigger =
  mkTriggerUnsafe (\ns t ns' -> RTrigger ns' t ns) ([Reversible],[Reversible])

putTrigger :: MonadHaltD m => NodeRef t -> PlanT t n m ()
putTrigger = putTransition <=< trigger
trigger :: Monad m => NodeRef t -> PlanT t n m (Transition t n)
trigger = mkTriggerUnsafe Trigger (fullRange,fullRange)

traverseTransition :: Applicative m =>
                 Side
               -> ([NodeRef n] -> m [NodeRef n])
               -> Transition t n -> m (Transition t n)
traverseTransition side f = \case
  RTrigger x y z -> case side of
    Inp -> (\x' -> RTrigger x' y z) <$> f x
    Out -> (\z' -> RTrigger x y z') <$> f z
  Trigger x y z -> case side of
    Inp -> (\x' -> Trigger x' y z) <$> f x
    Out -> (\z' -> Trigger x y z') <$> f z
  x -> pure x
transitionCost :: (MonadError (PlanningError t n) m,
                  MonadReader (GCConfig t n) m) =>
                 Transition t n -> m Cost
transitionCost = \case
  Trigger i _ o -> ioCost i o
  RTrigger i _ o -> ioCost i o
  DelNode n
    -> (\w -> Cost
        { costReads = 0
         ,costWrites = w
        }) <$> totalNodePages n -- invalidate each page
  where
    ioCost i o = do
      r <- sum <$> mapM totalNodePages i
      w <- sum <$> mapM totalNodePages o
      return $ Cost {costReads=r,costWrites=w}

getTransitions :: MonadReader (GCState t n) m => m [Transition t n]
getTransitions = reverse . (>>= transitions) . NEL.toList . epochs <$> ask
totalTransitionCost :: Monad m => PlanT t n m Cost
totalTransitionCost = dropReader get getTransitions
  >>= fmap mconcat . mapM transitionCost
