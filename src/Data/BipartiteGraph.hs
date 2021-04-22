{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf            #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE ViewPatterns          #-}
{-# OPTIONS_GHC -Wno-name-shadowing -Wno-orphans #-}

-- |NOTE: Incoming neighbors is the forward trigger input

-- | Bipartite graphs are graphs with two kinds of nodes. Here we
-- denote them with n and t. Roughly each n-node corresponds to a
-- relation and each t-node corresponds to a (part of an)
-- operation. Each n-node is only connected to t-nodes and visa
-- versa. Here we define the GraphBuilderT monad that we use to build
-- and maintain the graph. Each edge may be directed or
-- indirected. The n-nodes connected to a t-node are sematically split
-- in two non-empty groups: input and output nodes. Input connections
-- are n-to-t (directed) or underiected and output connections are
-- either t-to-n (directed) or undirected. We will see later that
-- plans are a trace of triggers of t-nodes whereby each trigger
-- assumes all input n-nodes are in a materialized state and which
-- materializes a subset of the output nodes. We can reverse-trigger a
-- t-node when the output nodes are all undirected (and therefore
-- reversible) in which case reverse-triggering materializes any
-- subset undirected input nodes.

module Data.BipartiteGraph
  ( Bipartite(..)
  , GraphBuilder
  , GraphBuilderT
  , GBState(..)
  , GBDiff(..)
  , NodeStruct
  , NodeStruct'(..)
  , toBGFunc
  , IsReversible(..)
  , Side(..)
  , sanityCheck
  , hoistGraphBuilderT
  -- We should only be able to append neighbors
  , getBottomNodesR
  , allNodesR
  , allNodesL
  , newNodeL
  , newNodeR
  , mergeNodesL
  , mergeNodesR
  , setNodeLinksL
  , setNodeLinksR
  , getNodeLinksL
  , getNodeLinksR
  , appendNodeLinksL
  , appendNodeLinksR
  , flipGraphBuilderT
  , nodeRefs
  , flipBipartite
  , runGraphBuilderT
  , runGraphBuilder
  , reprGraph
  , NodeRepr(..)
  , getAllLinksL
  , getAllLinksR
  , insertNodeRepr
  , pathsBetweenR
  , topNodes
  , botNodes
  , neighborSetR
  , neighborSetRN
  , depSetRN
  ) where

import           Control.Applicative
import           Control.Monad
import           Control.Monad.Identity
import           Control.Monad.Morph
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Trans.Maybe
import           Data.Bifunctor
import qualified Data.IntMap               as IM
import           Data.List
import           Data.Maybe
import           Data.Monoid
import           Data.NodeContainers
import           Data.Tuple
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.ListT
import           Data.Utils.MTL
import           Data.Utils.Ranges
import           GHC.Generics
import           Prelude                   hiding (filter, lookup)

-- # GRAPH
-- |Quick information access for nodes. This is for either t or n
newtype NodeStruct' i t a = NodeStruct {
  -- Incoming links that are only incoming
  nodeLinks   :: i -> (NodeSet t, NodeSet t)
  } deriving (Functor, Foldable, Traversable, Generic)
data IsReversible = Reversible | Irreversible
  deriving (Show, Eq, Enum, Bounded,Generic)
instance AShow IsReversible
instance ARead IsReversible
data Side = Inp | Out deriving (Show, Eq, Enum, Bounded)
flipSide :: Side -> Side
flipSide = \case
  Inp -> Out
  Out -> Inp

getSide :: Side -> (NodeSet t, NodeSet t) -> NodeSet t
getSide = \case
  Inp -> fst
  Out -> snd

modSide :: Side
        -> (NodeSet a -> NodeSet a)
        -> (NodeSet a, NodeSet a)
        -> (NodeSet a, NodeSet a)
modSide = \case
  Inp -> first
  Out -> second

getNodeLinks :: Side -> [IsReversible] -> NodeStruct l r -> NodeSet l
getNodeLinks sid revs nstruct =
  mconcat [getNodeLinks1 sid rev nstruct | rev <- revs]
getNodeLinks1 :: Side -> IsReversible -> NodeStruct l r -> NodeSet l
getNodeLinks1 sid rev = getSide sid . ($ rev) . nodeLinks
modNodeLinks :: Side
             -> IsReversible
             -> (NodeSet l -> NodeSet l)
             -> NodeStruct l r
             -> NodeStruct l r
modNodeLinks sid rev f ns@NodeStruct{..} =
  let (revVal, irrVal) =
        (case rev of {Reversible -> first; Irreversible -> second})
        (modSide sid f)
        $ (nodeLinks Reversible, nodeLinks Irreversible)
  in ns{
    nodeLinks=(\case
        Reversible -> revVal;
        Irreversible -> irrVal)
    }

overNodeSetL :: Monad m =>
              NodeSet l
            -> (NodeStruct r l -> NodeStruct r l)
            -> GraphBuilderT l r m ()
overNodeSetL ns f = forM_ (toNodeList ns) $ \ref -> do
  gbs <- get
  let bp = gbPropNet gbs
  let newNodes = refAdjust f ref $ lNodes bp
  put gbs{gbPropNet=bp{lNodes=newNodes}}

overNodeSetR :: Monad m =>
               NodeSet r
             -> (NodeStruct l r -> NodeStruct l r)
             -> GraphBuilderT l r m ()
overNodeSetR ns f = flipGraphBuilderT $ overNodeSetL ns f

-- |Query neighbors of an n-node. Treats tha n-node as if it were a
-- t-node.
getNodeLinksR :: Monad m =>
                Side
              -> [IsReversible]
              -> NodeRef n
              -> GraphBuilderT t n m (Maybe (NodeSet t))
getNodeLinksR s r = flipGraphBuilderT . getNodeLinksL s r
{-# INLINABLE getNodeLinksR #-}

-- | Query neighbors of a t-node
-- Get all neighbors
-- > getNodeLinksR fullRange fullRange ref
--
-- Get input nodes
-- > getNodeLinksR Inp fullRange ref
--
-- Get input directed (irreversible) nodes on output:
-- > getNodeLinksR Out Irreversible ref
getNodeLinksL :: Monad m =>
                Side
              -> [IsReversible]
              -> NodeRef t
              -> GraphBuilderT t n m (Maybe (NodeSet n))
getNodeLinksL s r = fmap2 (getNodeLinks s r) . dropReader get . getNodeStructL
{-# INLINABLE getNodeLinksL #-}

-- Safely set nodes
setNodeLinksR :: Monad m =>
                Side
              -> IsReversible
              -> NodeRef n
              -> NodeSet t
              -> GraphBuilderT t n m ()
setNodeLinksR side rev ref ns = flipGraphBuilderT
  $ setNodeLinksL side rev ref ns
setNodeLinksL :: Monad m =>
                Side
              -> IsReversible
              -> NodeRef t
              -> NodeSet n
              -> GraphBuilderT t n m ()
setNodeLinksL side rev ref ns = do
  nstructM <- dropReader get $ getNodeStructL ref
  case nstructM of
    Nothing      -> error "Node not found"
    Just nstruct -> go nstruct
  where
    go nstruct = do
      overNodeSetR (getNodeLinks side [rev] nstruct)
        $ modNodeLinks (flipSide side) rev
        $ \ns -> if ref `nsMember` ns then nsDelete ref ns else error "Oops"
      overNodeSetL (nsSingleton ref) $ modNodeLinks side rev (const ns)
      overNodeSetR ns $ modNodeLinks (flipSide side) rev $ nsInsert ref

appendNodeLinksR :: Monad m =>
                   Side
                 -> IsReversible
                 -> NodeRef r
                 -> NodeSet l
                 -> GraphBuilderT l r m ()
appendNodeLinksR side rev ref ns =
  flipGraphBuilderT $ appendNodeLinksL side rev ref ns
appendNodeLinksL :: Monad m =>
                   Side
                 -> IsReversible
                 -> NodeRef l
                 -> NodeSet r
                 -> GraphBuilderT l r m ()
appendNodeLinksL side rev ref ns = do
  linksM <- getNodeLinksL side [rev] ref
  case linksM of
    Nothing    -> error "Node not found."
    Just links -> setNodeLinksL side rev ref $ ns <> links

deleteNodeL :: Monad m => NodeRef l -> GraphBuilderT l r m ()
deleteNodeL ref = do
  forM_ fullRange2 $ \(sid,rev) -> setNodeLinksL sid rev ref mempty
  modify $ \gbs -> gbs{
    gbPropNet=(gbPropNet gbs){
        lNodes=refDelete ref $ lNodes (gbPropNet gbs)}}

#ifdef ENABLE_DELETE_NODES
deleteNodeR :: Monad m => NodeRef r -> GraphBuilderT l r m ()
deleteNodeR = flipGraphBuilderT . deleteNodeL
registerRmNodeL :: Monad m => NodeRef t -> GraphBuilderT t n m ()
registerRmNodeL ref = modify
  $ \gbs@GBState{ gbDiff=gbd@GBDiff{ gbDiffRmNodes=(nt, nn) }} ->
      gbs{gbDiff=gbd{gbDiffRmNodes=(ref:nt, nn)}}
#endif

registerNewNodeL :: Monad m => NodeRef t -> GraphBuilderT t n m ()
registerNewNodeL ref = modify
  $ \gbs@GBState{ gbDiff=gbd@GBDiff{ gbDiffNewNodes=(nt, nn) }} ->
      gbs{gbDiff=gbd{gbDiffNewNodes=(ref:nt, nn)}}

-- | Insert a node, if the node exists do nothing, if no node is
-- provided, make a new node. True if we created it , False if we didn't
newNodeL :: Monad m => Maybe (NodeRef t) -> GraphBuilderT t n m (Bool, NodeRef t)
newNodeL mref = do
  bp@Bipartite{lNodes=l} <- gbPropNet <$> get
  let mrefNext = fromMaybe ((+1) $ safeLast (NodeRef 0) $ refMapKeys l) mref
  if isJust $ mrefNext `refLU` l then return (False, mrefNext) else do
    let l' = refInsert mrefNext (NodeStruct (const (mempty, mempty))) l
    registerNewNodeL mrefNext
    putPropNet $ bp{lNodes=l'}
    return (True, mrefNext)
newNodeR :: Monad m => Maybe (NodeRef n) -> GraphBuilderT t n m (Bool, NodeRef n)
newNodeR = flipGraphBuilderT . newNodeL

type NodeStruct = NodeStruct' IsReversible
instance (AShow a, AShow i, Enum i, Bounded i) => AShow (NodeStruct' i t a) where
  ashow' (NodeStruct x) = sexp "NodeStruct" [ashowCase' x]
instance (ARead a, ARead i, Eq i) => ARead (NodeStruct' i t a) where
  aread' = \case
    Sub [Sym "NodeStruct",x] -> NodeStruct <$> areadCase' x
    _ -> Nothing

instance (Show a, Show i, Enum i, Bounded i) => Show (NodeStruct' i t a) where
  show NodeStruct{..} = printf
                        "NodeStruct {nodeLinks=%s}"
                        sNodeLinks
    where
      sNodeLinks :: String
      sNodeLinks = printf "(\\case {%s})"
                   $ intercalate "; "
                   [printf "%s -> %s" (show x) (show $ nodeLinks x)
                   | x <- fullRange]

data Bipartite t n = Bipartite {
  lNodes :: RefMap t (NodeStruct n t),
  rNodes :: RefMap n (NodeStruct t n)
  } deriving (Show, Generic)
instance (AShow t,AShow n) => AShow (Bipartite t n)
instance (ARead t,ARead n) => ARead (Bipartite t n)
instance Default (Bipartite t n)
instance Bifunctor Bipartite where
  bimap _ _ Bipartite{} = undefined
instance Functor (Bipartite l) where
  fmap = bimap id

flipGBState :: GBState t n -> GBState n t
flipGBState GBState{..} = GBState{
  gbPropNet=flipBipartite gbPropNet,
  gbDiff=flipGBDiff gbDiff
  }
{-# INLINE flipGBState #-}

flipBipartite :: Bipartite t n -> Bipartite n t
flipBipartite (Bipartite a b) = Bipartite b a
{-# INLINE flipBipartite #-}

flipGBDiff :: GBDiff t n -> GBDiff n t
flipGBDiff GBDiff{..} = GBDiff {
  gbDiffNewNodes=swap gbDiffNewNodes,
  gbDiffRmNodes=swap gbDiffRmNodes
  }
{-# INLINE flipGBDiff #-}

safeLast :: a -> [a] -> a
safeLast x = go where
  go []     = x
  go [a]    = a
  go (_:as) = go as

bipartiteIsEmpty :: Bipartite t n -> Bool
bipartiteIsEmpty (Bipartite x y) = null x && null y
{-# INLINE bipartiteIsEmpty #-}

instance Semigroup (Bipartite l r) where
  b1 <> b2 | bipartiteIsEmpty b1 = b2
          | bipartiteIsEmpty b2 = b1
          | otherwise =
              let
                lMin = safeLast (NodeRef 0) $ refMapKeys $ lNodes b1
                rMin = safeLast (NodeRef 0) $ refMapKeys $ rNodes b1
                -- XXX: rebase only the internal links.
                fullRebase :: (NodeRef b -> NodeRef b)
                           -> (NodeRef a -> NodeRef a)
                           -> RefMap b (NodeStruct a b)
                           -> RefMap b (NodeStruct a b)
                fullRebase rebk rebv =
                  fmap (rebaseVal rebv) . refMapKeysMonotonic rebk
                rebaseVal :: (NodeRef t -> NodeRef t)
                          -> NodeStruct t a
                          -> NodeStruct t a
                rebaseVal f NodeStruct{..} =
                  NodeStruct {
                    nodeLinks=biapply (nsMap f) <$> nodeLinks
                  }
              in
                Bipartite {
                  lNodes=lNodes b1 `refUnion`
                         fullRebase (+ lMin) (+ rMin) (lNodes b2),
                  rNodes=rNodes b1 `refUnion`
                         fullRebase (+ rMin) (+ lMin) (rNodes b2)}
instance Monoid (Bipartite l r) where
  mempty = Bipartite mempty mempty

nodeRefs :: Monad m => GraphBuilderT a b m ([NodeRef a], [NodeRef b])
nodeRefs = do
  (Bipartite x y) <- gbPropNet <$> get
  return (refKeys x, refKeys y)

data GBState a b = GBState {gbPropNet :: Bipartite a b, gbDiff :: GBDiff a b}
data GBDiff a b = GBDiff {
  -- New nodes added.
  gbDiffNewNodes :: ([NodeRef a], [NodeRef b]),
  -- Removed nodes.
  gbDiffRmNodes  :: ([NodeRef a], [NodeRef b])
  }
putPropNet :: MonadState (GBState t n) m => Bipartite t n -> m ()
putPropNet pn = modify $ \gbs -> gbs{gbPropNet=pn}

instance Semigroup (GBState a b) where
  (GBState x y) <> (GBState x' y') = GBState (x <> x') (y <> y')
instance Monoid (GBState a b) where mempty = GBState mempty mempty
instance Semigroup (GBDiff a b) where
  (GBDiff x y) <> (GBDiff x' y') = GBDiff (x <> x') (y <> y')
instance Monoid (GBDiff a b) where mempty = GBDiff mempty mempty
type GraphBuilderT a b m = StateT (GBState a b) m
hoistGraphBuilderT :: Monad m =>
                     (forall a . m a -> g a)
                   -> GraphBuilderT s t m a
                   -> GraphBuilderT s t g a
hoistGraphBuilderT = hoist

type GraphBuilder a b = GraphBuilderT a b Identity
runGraphBuilderT :: GraphBuilderT a b m x -> m (x, GBState a b)
runGraphBuilderT = (`runStateT` mempty)
runGraphBuilder :: GraphBuilder a b x -> (x, GBState a b)
runGraphBuilder = (`runState` mempty)

flipGraphBuilderT :: Functor m => GraphBuilderT a b m x -> GraphBuilderT b a m x
flipGraphBuilderT = StateT . flipBeforeAndAfter . runStateT where
  flipBeforeAndAfter f = fmap (second flipGBState) . f . flipGBState
{-# INLINE flipGraphBuilderT #-}

-- Check that the incoming/outgoing nodes match
sanityCheck :: Monad m => GraphBuilderT l r m Bool
sanityCheck = (&&) <$> sanityCheckL <*> flipGraphBuilderT sanityCheckL
  where
    sanityCheckL = do
      allNodes <- allNodesL
      everyM allNodes $ \ref -> everyM fullRange2 $ \(side, rev) -> do
        linksM <- getNodeLinksL side [rev] ref
        case linksM of
          Nothing -> error "allNodesL retured a link that we can't look up."
          Just links -> everyM (toNodeList links) $ \foreignRef -> do
            returningLinksM <- getNodeLinksR (flipSide side) [rev] foreignRef
            case returningLinksM of
              Nothing             -> return False
              Just returningLinks -> return $ ref `nsMember` returningLinks
    everyM :: forall m a . Monad m => [a] -> (a -> m Bool) -> m Bool
    everyM as = fmap and . forM as

getNodeStructR :: MonadReader (GBState t n) m =>
                 NodeRef n
               -> m (Maybe (NodeStruct t n))
getNodeStructR r = do
  Bipartite{rNodes=rn} <- gbPropNet <$> ask
  return $ r `refLU` rn
{-# INLINE getNodeStructR #-}
getNodeStructL :: MonadReader (GBState t n) m =>
                 NodeRef t
               -> m (Maybe (NodeStruct n t))
getNodeStructL = flipReaderGBState . getNodeStructR
{-# INLINE getNodeStructL #-}

flipReaderGBState :: MonadReader (GBState l r) m =>
                    ReaderT (GBState r l) m a -> m a
flipReaderGBState = dropReader (flipGBState <$> ask)
{-# INLINE flipReaderGBState #-}

biapply :: (a -> b) -> (a,a) -> (b,b)
biapply f = bimap f f

allNodesL :: Monad m => GraphBuilderT l r m [NodeRef l]
allNodesL = refKeys . lNodes . gbPropNet <$> get

allNodesR :: Monad m => GraphBuilderT l r m [NodeRef r]
allNodesR = refKeys . rNodes . gbPropNet <$> get

-- | All nodes with no incoming links.
getBottomNodesR :: GraphBuilder a b [NodeRef b]
getBottomNodesR = do
  Bipartite{rNodes=rn} <- gbPropNet <$> get
  return $ fmap fst $ filter (hasNoIncoming . snd) $ refAssocs rn
  where
    hasNoIncoming NodeStruct{..} =
      not $ any (nsNull . getSide Inp . nodeLinks) fullRange


-- Pairs denote (reversibe,non-reversible) nodes.
data NodeRepr t n = NodeRepr {
  nodeReprInput  :: ([NodeRef n], [NodeRef n]),
  nodeReprTNode  :: NodeRef t,
  nodeReprOutput :: ([NodeRef n], [NodeRef n])
  } deriving (Read, Show, Generic, Eq)

instance AShow (NodeRepr t n)

mergeNodesR :: Monad m => NodeRef r -> NodeRef r -> GraphBuilderT l r m ()
mergeNodesR ref1 ref2 = flipGraphBuilderT $ mergeNodesL ref1 ref2
mergeNodesL :: Monad m => NodeRef l -> NodeRef l -> GraphBuilderT l r m ()
mergeNodesL ref1 ref2 = forM_ fullRange2 $ \(side, ref) -> do
  linksM <- getNodeLinksL side [ref] ref2
  case linksM of
    Nothing    -> error "Link not found"
    Just links -> appendNodeLinksL side ref ref1 links
  deleteNodeL ref2

reprNode :: forall t n m . Monad m =>
           NodeRef t
         -> GraphBuilderT t n m (Maybe (NodeRepr t n))
reprNode nodeReprTNode = runMaybeT $ do
  nodeReprOutput <- onRevPair $ getLinksLocal Out
  nodeReprInput <- onRevPair $ getLinksLocal Inp
  return NodeRepr{..}
  where
    onRevPair f = (,) <$> f Reversible <*> f Irreversible
    getLinksLocal :: Side
                  -> IsReversible
                  -> MaybeT (GraphBuilderT t n m) [NodeRef n]
    getLinksLocal side x =
      fmap toNodeList
      $ MaybeT
      $ getNodeLinksL side [x] nodeReprTNode

reprGraph :: forall t n m . Monad m => GraphBuilderT t n m (Maybe [NodeRepr t n])
reprGraph = do
  tns :: [NodeRef t] <- fst <$> nodeRefs
  runMaybeT $ mapM (MaybeT . reprNode) tns

nodeReprNNodes :: Side -> IsReversible -> NodeRepr t n -> [NodeRef n]
nodeReprNNodes side rev NodeRepr{..} = takeSide nodeListPair where
  takeSide = case rev of
    Reversible   -> fst
    Irreversible -> snd
  nodeListPair = case side of
    Inp -> nodeReprInput
    Out -> nodeReprOutput

insertNodeRepr :: forall t n m . Monad m =>
                 NodeRepr t n -> GraphBuilderT t n m ()
insertNodeRepr nr = do
  void $ newNodeL (Just $ nodeReprTNode nr)
  forM_ fullRange2 $ \(side,rev) -> do
    let ns :: [NodeRef n] = nodeReprNNodes side rev nr
    forM_ ns $ \n -> newNodeR (Just n)
    setNodeLinksL side rev (nodeReprTNode nr) $ fromNodeList ns

toBGFunc :: GraphBuilderT t n Identity a
         -> Bipartite t n
         -> a
toBGFunc m net = evalState m $ GBState net mempty

getAllLinksR :: Monad m => Side -> NodeRef r -> GraphBuilderT l r m (NodeSet l)
getAllLinksR side ref = flipGraphBuilderT $ getAllLinksL side ref
getAllLinksL :: Monad m => Side -> NodeRef l -> GraphBuilderT l r m (NodeSet r)
getAllLinksL side ref =
  fmap mconcat $ forM fullRange $ \rev -> getNodeLinksL side [rev] ref >>= \case
    Nothing -> error "Not found"
    Just l -> return l

pathsBetweenR :: forall t n m . Monad m =>
                NodeRef n
              -> NodeRef n
              -> GraphBuilderT t n m [([NodeRef n], [NodeRef t])]
pathsBetweenR from to = if from == to then return [([from], [])] else runListT $ do
  (fromT, from') <- eachNeighbor Out from
  (toT, to') <- eachNeighbor Inp to
  (intermPathN, intermPathT) <- mkListT $ pathsBetweenR from' to'
  return $ ([from] ++ intermPathN ++ [to], [fromT] ++ intermPathT ++ [toT])
  where
    eachNeighbor :: Side
                 -> NodeRef n
                 -> ListT (GraphBuilderT t n m) (NodeRef t, NodeRef n)
    eachNeighbor side ref = mkListT $ fmap (>>= sequenceA) $ neighborSetR side ref

-- | (ts, ns) `in` neightborSetRN k s n means that if ALL ns were
-- materialized we could trigger all of ts and get n. Find ns on @s@
-- side of the computation. That means if @s@ is @Out@ then to make
-- @n@ we would reverse trigger. k is the MINIMUM radius.
neighborSetRN :: forall t n m .
                Monad m =>
                Int
              -> Side
              -> NodeRef n
              -> GraphBuilderT t n m [([NodeRef t], [NodeRef n])]
neighborSetRN 0 _ ref = return [([], [ref])]
neighborSetRN 1 side ref = fmap2 (first return) $ neighborSetR side ref
neighborSetRN i side ref = fmap3 nub $ fmap uniqMerge $ runListT $ do
  (tnode, interms) <- mkListT $ neighborSetR side ref
  restOfSolutionAlist <- lift $ forM interms
                        $ \ref -> (ref,) <$> neighborSetRN (i-1) side ref
  let (fmap fst -> finals, fmap snd -> partSolConj) =
        partition (null . snd) restOfSolutionAlist
  case partSolConj of
    [] -> mzero
    _  -> let partSolDisj = sequenceA partSolConj
         in fmap ((tnode:) `bimap` (finals ++))
            $ mkListT
            $ return
            $ map mconcat partSolDisj
  where
    uniqMerge :: Ord a => [([a],b)] -> [([a],b)]
    uniqMerge = foldr insertUniq [] . sortOn fst . fmap (first sort) where
      insertUniq (x,y) = \case
        [] -> [(x,y)]
        xs@((x',_):_) -> if x == x' then xs else (x,y):xs


neighborSetR :: forall t n m . Monad m =>
               Side
             -> NodeRef n
             -> GraphBuilderT t n m [(NodeRef t, [NodeRef n])]
neighborSetR side ref = runListT $ do
  neighbor :: NodeRef t <- mkListT $ possibleNeighbors ref
  lift
    $ fmap (neighbor,)
    $ flipGraphBuilderT
    $ possibleNeighbors neighbor
  where
    possibleNeighbors :: forall x y . NodeRef y
                      -> GraphBuilderT x y m [NodeRef x]
    possibleNeighbors = fmap (>>= toNodeList)
                        . fmap toList
                        . getNodeLinksR side fullRange

topNodes :: Monad m => GraphBuilderT t n m [NodeRef n]
topNodes = limitNodes Out
botNodes :: Monad m => GraphBuilderT t n m [NodeRef n]
botNodes = limitNodes Inp

limitNodes :: Monad m => Side -> GraphBuilderT t n m [NodeRef n]
limitNodes side =
  fmap fst
  . filter (nsNull
             . mconcat
             . fmap (getSide side)
             . (\f -> [f x | x <- fullRange])
             . nodeLinks
             . snd)
  . refAssocs
  . rNodes
  . gbPropNet
  <$> get

-- |Searching strictly in one direction get the possible dependency
-- sets of a node. A dependency set is the set of t-nodes that are to
-- be triggered and the set of n-nodes that need to be materialized
-- for this to work.
depSetRN :: forall t n m . Monad m =>
           Int
         -> Side
         -> NodeRef n
         -> GraphBuilderT t n m [([NodeRef t], [NodeRef n])]
depSetRN i side ref = evalStateT (go i ref) mempty where
  go :: Int
     -> NodeRef n
     -> StateT (IM.IntMap (RefMap n [([NodeRef t], [NodeRef n])]))
     (GraphBuilderT t n m)
     [([NodeRef t], [NodeRef n])]
  go = memoize $ \i ref -> case i of
    0 -> return [([], [ref])]
    1 -> do
      ret <- lift $ fmap2 (first return) $ neighborSetR side ref
      return $ if null ret then [([], [ref])] else ret
    _ -> do
      -- XXX: if a node has no dependencies return itself
      anyOf <- go (i-1) ref
      anyOfAnyOf :: [[([NodeRef t], [NodeRef n])]] <- forM anyOf $ \(ts, allOf) -> do
        allOfAnyOf :: [[([NodeRef t], [NodeRef n])]] <-  go (i-1) `traverse` allOf
        return $ first (ts ++) <$> allCombinations allOfAnyOf
      return $ fmap2 nub $ join anyOfAnyOf
  memoize :: Monad m' =>
            (Int -> NodeRef n -> StateT (IM.IntMap (RefMap n a)) m' a)
          -> (Int -> NodeRef n -> StateT (IM.IntMap (RefMap n a)) m' a)
  memoize m k r = do
    cache <- get
    let res = do {rmap <- IM.lookup k cache; refLU r rmap}
    case res of
      Just a -> return a
      Nothing -> do
        ret <- m k r
        modify $ IM.alter (Just . refInsert r ret . fromMaybe mempty) k
        return ret

allCombinations :: Monoid x => [[x]] -> [x]
allCombinations [] = []
allCombinations xs = foldr1 (liftM2 mappend) xs
