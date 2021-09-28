{-# LANGUAGE CPP                 #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE ViewPatterns        #-}

module Data.Cluster.PutCluster.Join
  (putJoinClusterC
  , mkJoinClustConfig
  , JoinClustConfig(..)
  , QRef(..)
  ) where

import           Control.Monad.Except
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Bipartite
import           Data.Cluster.ClusterConfig
import           Data.Cluster.Propagators
import           Data.Cluster.PutCluster.Common
import           Data.Cluster.Types
import           Data.Cluster.Types.Monad
import           Data.List.Extra
import           Data.NodeContainers
import           Data.QnfQuery.Build
import           Data.QnfQuery.BuildUtils
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.QuerySchema
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.Tup

data QRef n e s = QRef {getQRef :: NodeRef n, getNQNF :: NQNFQuery e s}
  deriving (Eq, Show)
type ShapeSymAssoc e s = [(ShapeSym e s,ShapeSym e s)]
data JoinClustConfig n e s = JoinClustConfig {
  assocL :: ShapeSymAssoc e s,
  assocR :: ShapeSymAssoc e s,
  assocO :: ShapeSymAssoc e s,
  jProp  :: Prop (Rel (Expr (ShapeSym e s))),
  qrefLO :: QRef n e s,
  qrefO  :: QRef n e s,
  qrefRO :: QRef n e s,
  qrefLI :: QRef n e s,
  qrefRI :: QRef n e s
  } deriving (Eq,Show)



-- | The JoinClustConfigs returned take all combinations of the
-- NQNFQueries to populate the jProp,assocs and nqnf aspect of qrefs
-- but the refernces should be all the same.
mkJoinClustConfig
  :: forall e s t n m .
  (Hashables2 e s,Monad m)
  => Prop (Rel (Expr e))
  -> (NodeRef n,[NQNFQueryI e s],Query e s)
  -> (NodeRef n,[NQNFQueryI e s],Query e s)
  -> CGraphBuilderT e s t n m [JoinClustConfig n e s]
mkJoinClustConfig p (lref,lnqnfs,ql) (rref,rnqnfs,qr) =
  fmap join
  $ (`evalStateT` (Nothing,Nothing,Nothing))
  $ forM ((,) <$> lnqnfs <*> rnqnfs)
  $ \(lnqnf,rnqnf) -> lift (nqnfJoinC p lnqnf rnqnf)
  >>= mapM (go lnqnf rnqnf)
  where
    go :: NQNFQueryI e s
       -> NQNFQueryI e s
       -> NQNFResultI (Prop (Rel (Expr (QNFName e s,e)))) e s
       -> StateT
         (Maybe (NodeRef n),Maybe (NodeRef n),Maybe (NodeRef n))
         (CGraphBuilderT e s t n m)
         (JoinClustConfig n e s)
    go lnqnf rnqnf resO = do
      let p' = nqnfResOrig resO
          resLO = nqnfLeftAntijoin p' lnqnf rnqnf
          resRO = nqnfRightAntijoin p' lnqnf rnqnf
          nqnfLO = nqnfPutQ QLeftAntijoin $ nqnfResNQNF resLO
          nqnfRO = nqnfPutQ QRightAntijoin $ nqnfResNQNF resRO
          nqnfO = nqnfPutQ QJoin $ nqnfResNQNF resO
          assocL = bimap mkP mkP <$> nqnfResInOutNames resLO
          assocR = bimap mkP mkP <$> nqnfResInOutNames resRO
          assocO = bimap mkP mkP <$> nqnfResInOutNames resO
      refLO <- cachedMkRef fst3 (first3 . const . Just) (nqnfToQnf nqnfLO)
      refO <- cachedMkRef snd3 (second3 . const . Just) (nqnfToQnf nqnfO)
      -- Note: QNFS and nodes are inserted but until they are connected
      -- to a cluster they can't be looked up. If LO and RO are eqaul
      -- then registerQNF can't detect that as it uses the cluisterbuild
      -- machinery. Also note that the only two nodes that can be equal
      -- here are LO and RO.
      refRO <- if nqnfToQnf nqnfLO == nqnfToQnf nqnfRO
        then modify (third3 $ const $ Just refLO) >> return refLO
        else cachedMkRef trd3 (third3 . const . Just) (nqnfToQnf nqnfRO)
      return
        JoinClustConfig
        { assocL = assocL
         ,assocR = assocR
         ,assocO = assocO
         ,jProp = fmap3 (uncurry mkShapeSym) p'
         ,qrefLO = QRef refLO nqnfLO
         ,qrefO = QRef refO nqnfO
         ,qrefRO = QRef refRO nqnfRO
         ,qrefLI = QRef lref $ second (putIdentityQNFQ ql) lnqnf
         ,qrefRI = QRef rref $ second (putIdentityQNFQ qr) rnqnf
        }
    nqnfPutQ
      :: (Prop (Rel (Expr e)) -> BQOp e) -> NQNFQueryI e s -> NQNFQuery e s
    nqnfPutQ constr = second $ putIdentityQNFQ $ Q2 (constr p) ql qr
    mkP :: (e,QNFCol e s) -> ShapeSym e s
    mkP (e,col) = mkShapeSym (Column col 0) e

-- Note: maybe we should not look for cached antijoins because join's
-- qnf is much more robust, but if we do we should be re-locating the
-- noderefs of the antijoins.
putJoinClusterC :: forall e s t n m . (Hashables2 e s, Monad m) =>
                 JoinClustConfig n e s
               -> CGraphBuilderT e s t n m (JoinClust e s t n)
putJoinClusterC jcc = do
  c <- idempotentClusterInsert constr $ putJoinClusterI jcc
  forM_ [qrefLO jcc,qrefO jcc,qrefRO jcc] $ \nqnf -> do
    linkQnfClust (nqnfToQnf $ getNQNF nqnf) $ JoinClustW c
  putJClustProp (assocL jcc,assocO jcc,assocR jcc) (jProp jcc) c
  return c
  where
    constr =
      [(binClusterLeftIn . joinBinCluster,getQRef $ qrefLI jcc)
      ,(binClusterRightIn . joinBinCluster,getQRef $ qrefRI jcc)
      ,(binClusterOut . joinBinCluster,getQRef $ qrefO jcc)
      ,(joinClusterLeftAntijoin,getQRef $ qrefLO jcc)
      ,(joinClusterRightAntijoin,getQRef $ qrefRO jcc)]

-- | This is used only for intermediate nodes that we know we wont match.
mkQNF :: (Monad m,Hashables2 e s)
      => Query (ShapeSym e s) (NQNFQuery e s)
      -> CGraphBuilderT e s t n m (QNFQuery e s)
mkQNF (first shapeSymOrig -> q) = do
  res <- mkNQNF q
  let qnf = putIdentityQNFQ (qnfOrigDEBUG . snd =<< q) $ snd $ nqnfResNQNF res
  return qnf

-- | Make an NQNF. This is for building and therefore we just drop the
-- query plan.
mkNQNF :: (Hashables2 e s,Monad m,HasCallStack)
       => Query e (NQNFQuery e s)
       -> CGraphBuilderT e s t n m (NQNFResultI (Query (QNFName e s,e) ()) e s)
mkNQNF q = do
  qnfCache <- gets $ qnfBuildCache . clustBuildCache
  (ret,qnfCache') <- liftQnfError
    $ return
    $ (`runStateT` qnfCache)
    $ fmap (maximumOn (hash . snd . nqnfResNQNF))
    $ runListT
    $ toNQNFQuery
    $ (,()) . second putEmptyQNFQ <$> q
  modify $ \s -> s
    { clustBuildCache = (clustBuildCache s) { qnfBuildCache = qnfCache' } }
  return ret

liftQnfError :: Monad m =>
               m (Either (QNFError e s) a)
             -> ExceptT (ClusterError e s) m a
liftQnfError = ExceptT . fmap (first ClusterQNFError)

(.<~.) :: Monad m => NodeRef t -> NodeRef n -> GraphBuilderT t n m ()
outT .<~. inN =
  linkNodes
    NodeLinkDescr
    { nldNSide = Inp,nldIsRev = Irreversible,nldTNode = outT,nldNNode = inN }

(.<<~>.) :: Monad m => NodeRef t -> [NodeRef n] -> GraphBuilderT t n m ()
outT .<<~>. inNs = forM_ inNs $ \inN -> do
  linkNodes
    NodeLinkDescr
    { nldNSide = Inp,nldIsRev = Reversible,nldTNode = outT,nldNNode = inN }

(.<~>>.) :: Monad m => NodeRef t -> [NodeRef n] -> GraphBuilderT t n m ()
inT .<~>>. outNs = forM_ outNs $ \outN -> do
  linkNodes
    NodeLinkDescr
    { nldNSide = Out,nldIsRev = Reversible,nldTNode = inT,nldNNode = outN }

-- Connect 3 nodes with a join cluster
putJoinClusterI :: forall e s t n  m . (Hashables2 e s, Monad m) =>
                 JoinClustConfig n e s
               -> CGraphBuilderT e s t n m (JoinClust e s t n)
putJoinClusterI JoinClustConfig {..} = do
  -- Node
  let qnfO = nqnfToQnf $ getNQNF qrefO
      qnfLO = nqnfToQnf $ getNQNF qrefLO
      qnfRO = nqnfToQnf $ getNQNF qrefRO
  qnfLInterm <- mkQNF
    $ Q2 QProjQuery (Q0 $ getNQNF qrefLI) (Q0 $ getNQNF qrefO)
  qnfRInterm <- mkQNF
    $ Q2 QProjQuery (Q0 $ getNQNF qrefRI) (Q0 $ getNQNF qrefO)
  tRef <- mkNodeFromQnfT qnfO
  lSplit <- mkNodeFromQnfT qnfLO
  rSplit <- mkNodeFromQnfT qnfRO
  -- Intermediates should not be shared between clusters
  lInterm <- mkNodeFormQnfNUnsafe qnfLInterm
  rInterm <- mkNodeFormQnfNUnsafe qnfRInterm
  -- Structure
  lift2 $ do
    lSplit .<<~>. [getQRef qrefLI]
    lSplit .<~. getQRef qrefRI
    rSplit .<<~>. [getQRef qrefRI]
    rSplit .<~. getQRef qrefLI
    tRef .<<~>. [lInterm,rInterm]
    rSplit .<~>>. [rInterm,getQRef qrefRO]
    lSplit .<~>>. [getQRef qrefLO,lInterm]
    tRef .<~>>. [getQRef qrefO]
  let clust :: JoinClust e s t n =
        updateCHash
          JoinClust
          { joinBinCluster = updateCHash
              BinClust
              { binClusterLeftIn = noopRef $ getQRef qrefLI
               ,binClusterRightIn = noopRef $ getQRef qrefRI
               ,binClusterOut = withOpRef (QJoin jProp) $ getQRef qrefO
               ,binClusterT = tRef
               ,binClusterHash = undefined
              }
           ,joinClusterLeftAntijoin = withOpRef (QLeftAntijoin jProp)
              $ getQRef qrefLO
           ,joinClusterRightAntijoin = withOpRef (QRightAntijoin jProp)
              $ getQRef qrefRO
           ,joinClusterLeftIntermediate = noopRef lInterm
           ,joinClusterRightIntermediate = noopRef rInterm
           ,joinClusterLeftSplit = lSplit
           ,joinClusterRightSplit = rSplit
           ,joinClusterHash = undefined
          }
  forM_ [getQRef qrefLI,getQRef qrefRI]
    $ \ref -> registerClusterInput ref $ JoinClustW clust
  -- We normally link qnfs outside of the context of this function but
  -- these are intermediates.
  forM_ [qnfLInterm,qnfRInterm] $ \qnf -> linkQnfClust qnf $ JoinClustW clust
  return clust

-- PUTPROP
putJClustProp
  :: (HasCallStack,Hashables2 e s,Monad m)
  => ([(ShapeSym e s,ShapeSym e s)]
     ,[(ShapeSym e s,ShapeSym e s)]
     ,[(ShapeSym e s,ShapeSym e s)])
  -> Prop (Rel (Expr (ShapeSym e s)))
  -> JoinClust e s t n
  -> CGraphBuilderT e s t n m ()
putJClustProp assocs@(l,o,r) prop clust =
  putShapePropagator (JoinClustW clust)
  $ ACPropagatorAssoc
  { acpaPropagator = cPropToACProp $ joinClustPropagator assocs prop
   ,acpaInOutAssoc = l ++ o ++ r
  }
