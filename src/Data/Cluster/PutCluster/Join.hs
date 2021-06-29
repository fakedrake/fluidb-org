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
import           Data.CnfQuery.Build
import           Data.CnfQuery.BuildUtils
import           Data.CnfQuery.Types
import           Data.List.Extra
import           Data.NodeContainers
import           Data.Query.Algebra
import           Data.Query.QuerySchema
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.Tup

data QRef n e s = QRef {getQRef :: NodeRef n, getNCNF :: NCNFQuery e s}
  deriving (Eq, Show)
type PlanSymAssoc e s = [(PlanSym e s,PlanSym e s)]
data JoinClustConfig n e s = JoinClustConfig {
  assocL :: PlanSymAssoc e s,
  assocR :: PlanSymAssoc e s,
  assocO :: PlanSymAssoc e s,
  jProp  :: Prop (Rel (Expr (PlanSym e s))),
  qrefLO :: QRef n e s,
  qrefO  :: QRef n e s,
  qrefRO :: QRef n e s,
  qrefLI :: QRef n e s,
  qrefRI :: QRef n e s
  } deriving (Eq,Show)



-- | The JoinClustConfigs returned take all combinations of the
-- NCNFQueries to populate the jProp,assocs and ncnf aspect of qrefs
-- but the refernces should be all the same.
mkJoinClustConfig
  :: forall e s t n m .
  (Hashables2 e s,Monad m)
  => Prop (Rel (Expr e))
  -> (NodeRef n,[NCNFQueryI e s],Query e s)
  -> (NodeRef n,[NCNFQueryI e s],Query e s)
  -> CGraphBuilderT e s t n m [JoinClustConfig n e s]
mkJoinClustConfig p (lref,lncnfs,ql) (rref,rncnfs,qr) =
  fmap join
  $ (`evalStateT` (Nothing,Nothing,Nothing))
  $ forM ((,) <$> lncnfs <*> rncnfs)
  $ \(lncnf,rncnf) -> lift (ncnfJoinC p lncnf rncnf)
  >>= mapM (go lncnf rncnf)
  where
    go :: NCNFQueryI e s
       -> NCNFQueryI e s
       -> NCNFResultI (Prop (Rel (Expr (CNFName e s,e)))) e s
       -> StateT
         (Maybe (NodeRef n),Maybe (NodeRef n),Maybe (NodeRef n))
         (CGraphBuilderT e s t n m)
         (JoinClustConfig n e s)
    go lncnf rncnf resO = do
      let p' = ncnfResOrig resO
          resLO = ncnfLeftAntijoin p' lncnf rncnf
          resRO = ncnfRightAntijoin p' lncnf rncnf
          ncnfLO = ncnfPutQ QLeftAntijoin $ ncnfResNCNF resLO
          ncnfRO = ncnfPutQ QRightAntijoin $ ncnfResNCNF resRO
          ncnfO = ncnfPutQ QJoin $ ncnfResNCNF resO
          assocL = bimap mkP mkP <$> ncnfResInOutNames resLO
          assocR = bimap mkP mkP <$> ncnfResInOutNames resRO
          assocO = bimap mkP mkP <$> ncnfResInOutNames resO
      refLO <- cachedMkRef fst3 (first3 . const . Just) (ncnfToCnf ncnfLO)
      refO <- cachedMkRef snd3 (second3 . const . Just) (ncnfToCnf ncnfO)
      -- Note: CNFS and nodes are inserted but until they are connected
      -- to a cluster they can't be looked up. If LO and RO are eqaul
      -- then registerCNF can't detect that as it uses the cluisterbuild
      -- machinery. Also note that the only two nodes that can be equal
      -- here are LO and RO.
      refRO <- if ncnfToCnf ncnfLO == ncnfToCnf ncnfRO
        then modify (third3 $ const $ Just refLO) >> return refLO
        else cachedMkRef trd3 (third3 . const . Just) (ncnfToCnf ncnfRO)
      return
        JoinClustConfig
        { assocL = assocL
         ,assocR = assocR
         ,assocO = assocO
         ,jProp = fmap3 (uncurry mkPlanSym) p'
         ,qrefLO = QRef refLO ncnfLO
         ,qrefO = QRef refO ncnfO
         ,qrefRO = QRef refRO ncnfRO
         ,qrefLI = QRef lref $ second (putIdentityCNFQ ql) lncnf
         ,qrefRI = QRef rref $ second (putIdentityCNFQ qr) rncnf
        }
    ncnfPutQ
      :: (Prop (Rel (Expr e)) -> BQOp e) -> NCNFQueryI e s -> NCNFQuery e s
    ncnfPutQ constr = second $ putIdentityCNFQ $ Q2 (constr p) ql qr
    mkP :: (e,CNFCol e s) -> PlanSym e s
    mkP (e,col) = mkPlanSym (Column col 0) e

-- Note: maybe we should not look for cached antijoins because join's
-- cnf is much more robust, but if we do we should be re-locating the
-- noderefs of the antijoins.
putJoinClusterC :: forall e s t n m . (Hashables2 e s, Monad m) =>
                 JoinClustConfig n e s
               -> CGraphBuilderT e s t n m (JoinClust e s t n)
putJoinClusterC jcc = do
  c <- idempotentClusterInsert constr $ putJoinClusterI jcc
  forM_ [qrefLO jcc,qrefO jcc,qrefRO jcc] $ \ncnf -> do
    linkCnfClust (ncnfToCnf $ getNCNF ncnf) $ JoinClustW c
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
mkCNF :: (Monad m,Hashables2 e s)
      => Query (PlanSym e s) (NCNFQuery e s)
      -> CGraphBuilderT e s t n m (CNFQuery e s)
mkCNF (first planSymOrig -> q) = do
  res <- mkNCNF q
  let cnf = putIdentityCNFQ (cnfOrigDEBUG . snd =<< q) $ snd $ ncnfResNCNF res
  return cnf

-- | Make an NCNF. This is for building and therefore we just drop the
-- query plan.
mkNCNF :: (Hashables2 e s,Monad m,HasCallStack)
       => Query e (NCNFQuery e s)
       -> CGraphBuilderT e s t n m (NCNFResultI (Query (CNFName e s,e) ()) e s)
mkNCNF q = do
  cnfCache <- gets $ cnfBuildCache . clustBuildCache
  (ret,cnfCache') <- liftCnfError
    $ return
    $ (`runStateT` cnfCache)
    $ fmap (maximumOn (hash . snd . ncnfResNCNF))
    $ runListT
    $ toNCNFQuery
    $ (,()) . second putEmptyCNFQ <$> q
  modify $ \s -> s
    { clustBuildCache = (clustBuildCache s) { cnfBuildCache = cnfCache' } }
  return ret

liftCnfError :: Monad m =>
               m (Either (CNFError e s) a)
             -> ExceptT (ClusterError e s) m a
liftCnfError = ExceptT . fmap (first ClusterCNFError)

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
  let cnfO = ncnfToCnf $ getNCNF qrefO
      cnfLO = ncnfToCnf $ getNCNF qrefLO
      cnfRO = ncnfToCnf $ getNCNF qrefRO
  cnfLInterm <- mkCNF
    $ Q2 QProjQuery (Q0 $ getNCNF qrefLI) (Q0 $ getNCNF qrefO)
  cnfRInterm <- mkCNF
    $ Q2 QProjQuery (Q0 $ getNCNF qrefRI) (Q0 $ getNCNF qrefO)
  tRef <- mkNodeFromCnfT cnfO
  lSplit <- mkNodeFromCnfT cnfLO
  rSplit <- mkNodeFromCnfT cnfRO
  -- Intermediates should not be shared between clusters
  lInterm <- mkNodeFormCnfNUnsafe cnfLInterm
  rInterm <- mkNodeFormCnfNUnsafe cnfRInterm
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
  -- We normally link cnfs outside of the context of this function but
  -- these are intermediates.
  forM_ [cnfLInterm,cnfRInterm] $ \cnf -> linkCnfClust cnf $ JoinClustW clust
  return clust

-- PUTPROP
putJClustProp :: (HasCallStack,Hashables2 e s, Monad m) =>
                ([(PlanSym e s,PlanSym e s)],
                 [(PlanSym e s,PlanSym e s)],
                 [(PlanSym e s,PlanSym e s)])
              -> Prop (Rel (Expr (PlanSym e s)))
              -> JoinClust e s t n
              -> CGraphBuilderT e s t n m ()
putJClustProp assocs@(l,o,r) prop clust = putPlanPropagator
    (JoinClustW clust)
    (cPropToACProp $ joinClustPropagator assocs prop,l ++ o ++ r)
