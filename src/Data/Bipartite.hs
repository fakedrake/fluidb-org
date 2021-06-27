module Data.Bipartite
  (Bipartite
  ,nNodes
  ,GraphBuilderT
  ,GBState(gbPropNet)
  ,IsReversible(..)
  ,Side(..)
  ,NodeLinksFilter(..)
  ,NodeLinkDescr(..)
  ,hoistGraphBuilderT
  ,getBottomNodesN
  ,newNodeN
  ,newNodeT
  ,getNodeLinksN
  ,getNodeLinksT
  ,linkNodes
  ,nodeRefs
  ,getAllLinksN
  ,getAllLinksT) where

import           Control.Monad.Morph
import           Control.Monad.State
import           Data.Bifunctor
import           Data.NodeContainers
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Ranges
import           GHC.Generics
import           Unsafe.Coerce


-- | All information about the connection of a node to the rest of the
-- graph is accessible by this data structure.
data NodeLinksTo a =
  NodeLinksTo
  {
    -- | Each of these nodes are on the tip of an reversible arrow.
    nlRevOut   :: NodeSet a
   ,
    -- | Each of these nodes are on the tip of a non-reversible arrow.
    nlIrrevOut :: NodeSet a
   ,
    -- | Each of these nodes are on the butt of a reversibel arrow.
    nlRevIn    :: NodeSet a
   ,
    -- | Each of these nodes are on the butt of a non-reversible
    -- arrow.
    nlIrrevIn  :: NodeSet a
  }
  deriving (Show,Read,Generic)
instance Default (NodeLinksTo a)
instance AShow (NodeLinksTo a)
instance ARead (NodeLinksTo a)

data Bipartite t n =
  Bipartite { bigraphTNodes :: RefMap t (NodeLinksTo n)
             ,bigraphNNodes :: RefMap n (NodeLinksTo t)
            }
  deriving (Show,Read,Generic)
instance Bifunctor Bipartite where
  bimap _ _ = unsafeCoerce
instance Default (Bipartite t n)
instance AShow (Bipartite t n)
instance ARead (Bipartite t n)

newtype GBState t n = GBState { gbPropNet :: Bipartite t n }
  deriving Generic
instance Default (GBState t n)
instance AShow (GBState t n)
instance ARead (GBState t n)
type GraphBuilderT t n m = StateT (GBState t n) m

-- A node being on out means the arrow is pointing at it.
data Side = Inp | Out deriving (Show, Eq, Enum, Bounded)

data IsReversible
  = Reversible
  | Irreversible
  deriving (Show,Eq,Enum,Bounded,Generic,Read)

data NodeLinksFilter a =
  NodeLinksFilter
  {
    -- | The node we are querying about
    nlfNode  :: NodeRef a,
    -- | Out means we are looking for nodes that are being pointed to,
    -- ie that nlfNode is on the butt side of the arrow.
    nlfSide  :: [Side],
    -- | Are we looking for reversible or non-reversible links?
    nlfIsRev :: [IsReversible] }

data NodeLinkDescr t n =
  NodeLinkDescr
  { nldTNode :: NodeRef t
   ,nldNNode :: NodeRef n
   ,nldNSide :: Side -- Out means the link is pointing to the n-node
   ,nldIsRev :: IsReversible
  }

-- Interface
hoistGraphBuilderT
  :: Monad m
  => (forall x . m x -> g x)
  -> GraphBuilderT s t m a
  -> GraphBuilderT s t g a
hoistGraphBuilderT = hoist

-- | Create a new Unconnected node.
newNodeT :: Monad m => GraphBuilderT t n m (NodeRef t)
newNodeT = do
  tnodes <- gets $ bigraphTNodes . gbPropNet
  let mrefNext = maybe 0 ((+ 1) . fst) $ refLookupMax tnodes
  modify $ \s -> s
    { gbPropNet = (gbPropNet s)
        { bigraphTNodes = refInsert mrefNext def tnodes }
    }
  return mrefNext

newNodeN :: Monad m => GraphBuilderT t n m (NodeRef n)
newNodeN = do
  nnodes <- gets $ bigraphNNodes . gbPropNet
  let mrefNext = maybe 0 ((+ 1) . fst) $ refLookupMax nnodes
  modify $ \s -> s
    { gbPropNet = (gbPropNet s)
        { bigraphNNodes = refInsert mrefNext def nnodes }
    }
  return mrefNext

getBottomNodesN :: Monad m => GraphBuilderT t n m [NodeRef n]
getBottomNodesN = do
  nmap <- gets $ refAssocs . bigraphNNodes . gbPropNet
  return [n | (n,v) <- nmap,emptyLinks v]

nodeRefs :: Monad m => GraphBuilderT t n m (NodeSet t,NodeSet n)
nodeRefs = do
  g <- gets gbPropNet
  return (refToNodeSet $ bigraphTNodes g,refToNodeSet $ bigraphNNodes g)

getAllLinksT :: Monad m => Side -> NodeRef t -> GraphBuilderT t n m (NodeSet n)
getAllLinksT side ref = do
  s <- getNodeLinksT
    $ NodeLinksFilter { nlfNode = ref,nlfSide = [side],nlfIsRev = fullRange }
  case s of
    Nothing -> error $ "No links for node " ++ show ref
    Just x  -> return x

getAllLinksN :: Monad m => Side -> NodeRef n -> GraphBuilderT t n m (NodeSet t)
getAllLinksN side ref = do
  s <- getNodeLinksN
    $ NodeLinksFilter { nlfNode = ref,nlfSide = [side],nlfIsRev = fullRange }
  case s of
    Nothing -> error $ "No links for node " ++ show ref
    Just x  -> return x

nNodes :: Bipartite t n -> [NodeRef n]
nNodes = toNodeList . refToNodeSet . bigraphNNodes

-- | Query neighbors of a t-node
-- Get all neighbors
-- > getNodeLinksR fullRange fullRange ref
--
-- Get input nodes
-- > getNodeLinksR Inp fullRange ref
--
-- Get input directed (irreversible) nodes on output:
-- > getNodeLinksR Out Irreversible ref
getNodeLinksT
  :: Monad m => NodeLinksFilter t -> GraphBuilderT t n m (Maybe (NodeSet n))
getNodeLinksT flt@NodeLinksFilter {..} =
  gets
  $ fmap (runNodeLinksFilter flt) . refLU nlfNode . bigraphTNodes . gbPropNet
{-# INLINABLE getNodeLinksT #-}

getNodeLinksN
  :: Monad m => NodeLinksFilter n -> GraphBuilderT t n m (Maybe (NodeSet t))
getNodeLinksN flt@NodeLinksFilter {..} =
  gets
  $ fmap (runNodeLinksFilter flt) . refLU nlfNode . bigraphNNodes . gbPropNet
{-# INLINABLE getNodeLinksN #-}


linkNodes :: Monad m => NodeLinkDescr t n -> GraphBuilderT t n m ()
linkNodes NodeLinkDescr {..} = do
  modify $ \s -> s
    { gbPropNet = (gbPropNet s)
        { bigraphTNodes =
            refAdjust (putNode nldNSide nldIsRev nldNNode) nldTNode
            $ bigraphTNodes
            $ gbPropNet s
         ,bigraphNNodes =
            refAdjust (putNode (flipSide nldNSide) nldIsRev nldTNode) nldNNode
            $ bigraphNNodes
            $ gbPropNet s
        }
    }
  where
    -- put a node in the node-links structure. For Out on side means
    -- that node is at the tip of the node-links.
    putNode side isRev node nl = case (isRev,side) of
      (Irreversible,Inp) -> nl { nlIrrevIn = nsInsert node $ nlIrrevIn nl }
      (Reversible,Inp)   -> nl { nlRevIn = nsInsert node $ nlRevIn nl }
      (Irreversible,Out) -> nl { nlIrrevOut = nsInsert node $ nlIrrevOut nl }
      (Reversible,Out)   -> nl { nlRevOut = nsInsert node $ nlRevOut nl }
    flipSide Inp = Out
    flipSide Out = Inp
{-# INLINABLE linkNodes #-}



-- Internals

-- | Apply the filter to the links of a node. Remember that Out in the
-- filter means that we are looking for nodes that are on the tip of
-- an arrow.
runNodeLinksFilter :: NodeLinksFilter a -> NodeLinksTo b -> NodeSet b
runNodeLinksFilter NodeLinksFilter {..} NodeLinksTo {..} =
  mconcat $ go <$> ((,) <$> nlfIsRev <*> nlfSide)
  where
    go = \case
      (Irreversible,Inp) -> nlIrrevIn
      (Reversible,Inp)   -> nlRevIn
      (Irreversible,Out) -> nlIrrevOut
      (Reversible,Out)   -> nlRevOut

emptyLinks :: NodeLinksTo n -> Bool
emptyLinks NodeLinksTo {..} =
  nsNull nlIrrevIn && nsNull nlRevIn && nsNull nlIrrevOut && nsNull nlRevOut

listLinks :: Bipartite t n -> [NodeLinkDescr t n]
listLinks g = do
  (nref,NodeLinksTo {..}) <- refAssocs $ bigraphNNodes g
  (nSide,isRev,trefSet)
    <- [(Out,Irreversible,nlIrrevOut)
       ,(Out,Reversible,nlRevOut)
       ,(Inp,Irreversible,nlIrrevIn)
       ,(Inp,Reversible,nlRevIn)]
  tref <- toNodeList trefSet
  return
    NodeLinkDescr
    { nldNNode = nref,nldTNode = tref,nldIsRev = isRev,nldNSide = nSide }
