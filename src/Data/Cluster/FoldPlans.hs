{-# LANGUAGE DeriveFoldable      #-}
{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DeriveTraversable   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

module Data.Cluster.FoldPlans (queryPlans1,querySize,Query1(..)) where

import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Cluster.ClusterConfig
import           Data.Cluster.Propagators
import           Data.Cluster.Types.Clusters
import           Data.Cluster.Types.Monad
import           Data.Codegen.Build.Types
import           Data.List.Extra
import qualified Data.List.NonEmpty                as NEL
import           Data.Maybe
import           Data.NodeContainers
import           Data.Proxy
import           Data.Query.Algebra
import           Data.Query.QuerySchema.SchemaBase
import           Data.Query.QuerySchema.Types
import           Data.Query.QuerySize
import           Data.Utils.AShow
import           Data.Utils.Function
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.MTL
import           Data.Utils.Unsafe
import           GHC.Generics
import           Text.Printf

-- | All the different operators that correspond to materialize
-- NodeRef using cluster.
clusterOp
  :: forall c e s t n .
  SpecificCluster c
  => Proxy (PlanSym e s)
  -> NodeRef n
  -> c NodeRef (ComposedType c (PlanSym e s) NodeRef) t n
  -> [ClusterOp c (PlanSym e s)]
clusterOp _ ref =
  -- Drop the ref part of the pairs
  concatMap fst
  -- We are only interested in the ones where ref is the one we are
  -- looking for
  . filter ((== ref) . snd)
  -- For each one get the (operator, ref) pair
  . fmap (extractRef (Proxy :: Proxy c) (Proxy :: Proxy (PlanSym e s)))
  -- get the n-nodes
  . snd . allNodeRefs0


data Query1 e s = Q1' (UQOp e) s | Q2' (BQOp e) s s
  deriving (Functor,Traversable,Foldable,Generic)
instance (AShow e,AShow s)
  => AShow (Query1 e s)
queryPlans1
  :: forall e s t n m .
  (Hashables2 e s,MonadReader (ClusterConfig e s t n) m)
  => NodeRef n
  -> m [([Query1 (PlanSym e s) (NodeRef n)],AnyCluster e s t n)]
queryPlans1 refO = do
  clusts <- fmap join $ fmap2 snd $ getClustersNonInput refO
  forM clusts $ \clust -> fmap (,clust) $ case clust of
    JoinClustW c -> getQueryRecurse2 (clusterInputs clust) c
    BinClustW c -> getQueryRecurse2 (clusterInputs clust) c
    UnClustW c -> case clusterInputs clust of
      [inRef] -> return
        [Q1' op inRef | op <- clusterOp (Proxy :: Proxy (PlanSym e s)) refO c]
      _ -> inpNumError "UnClust" $ length $ clusterInputs clust
    NClustW (NClust _) -> return []
  where
    getQueryRecurse2
      :: forall c' .
      (ClusterOp c' (PlanSym e s) ~ BQOp (PlanSym e s),SpecificCluster c')
      => [NodeRef n]
      -> c' NodeRef (ComposedType c' (PlanSym e s) NodeRef) t n
      -> m [Query1 (PlanSym e s) (NodeRef n)]
    getQueryRecurse2 inps c = case inps of
      [lref,rref] -> return [Q2' op lref rref | op <- binOps]
      _           -> inpNumError "BinClust or JoinClust" $ length inps
      where
        binOps = clusterOp (Proxy :: Proxy (PlanSym e s)) refO c
    inpNumError :: String -> Int -> m x
    inpNumError s n =
      error $ printf "clusterInpts returns %d elements over %s" n s

querySize1
  :: forall e s t n m .
  (Hashables2 e s
  ,MonadError (SizeInferenceError e s t n) m
   -- Nothing in a node means we are currently looking up the node
  ,MonadState (ClusterConfig e s t n,RefMap n (Maybe ([TableSize],Double))) m)
  => AnyCluster e s t n
  -> Query1 (PlanSym e s) (([TableSize],Double),QueryPlan e s)
  -> m ([TableSize],Double)
querySize1 clust q = do
  ((_,assoc),planClust) <- dropReader (gets fst)
    $ getValidClustPropagator clust
  dropState (gets fst,modify . first . const) $ putPlanCluster planClust
  case q of
    Q2' o l r -> inferBinQuerySizeN o <$> transl o assoc l
      <*> transl o (reverse assoc) r
    Q1' o l -> return $ inferUnQuerySizeN o $ fst l
  where
    -- |When choosing symbol for join it is always the second symbol
    -- we find as the first will refer to the antijoin
    chooseSym = \case
      QJoin _ -> listToMaybe . safeTail
      _       -> listToMaybe
    transl :: BQOp x
           -> [(PlanSym e s,PlanSym e s)]
           -> (([TableSize],Double),QueryPlan e s)
           -> m (([TableSize],Double),QueryPlan e s)
    transl o assoc plan =
      either (const $ throwAStr $ "Op: " ++ ashow (plan,assoc)) return
      $ traverse (translatePlanMap'' (chooseSym o) assoc) plan

querySize :: forall e s t n m .
            (Hashables2 e s,
             MonadError (SizeInferenceError e s t n) m,
             -- Nothing in a node means we are currently looking up the node
             MonadState (ClusterConfig e s t n,
                         RefMap n (Maybe ([TableSize],Double)))
             m) =>
            NodeRef n -> m (([TableSize],Double),QueryPlan e s)
querySize ref = get >>= (\case
  Just (Just x) -> withPlan x
  Just Nothing -> do
    cs :: [([Query1 (PlanSym e s) (NodeRef n)],AnyCluster e s t n)] <-
      dropReader (fst <$> get) (queryPlans1 ref)
    -- Note: in the clusters appearing here `ref` is a non-input node.
    throwAStr $ "Cycle: " ++ ashow (ref,cs)
  Nothing -> dropReader (fst <$> get) (queryPlans1 ref) >>= \case
    [] -> throwAStr $ "Size of bot node should be in cache: " ++ show ref
    clusts -> getMedian $ takeListT 100 $ do
      (q1s,clust) <- mkListT $ return clusts
      q1 <- maybe mzero return $ listToMaybe q1s
      modify $ second $ refInsert ref Nothing
      -- querySize1 also triggers the propagator for the plans.
      ret <- querySize1 clust =<< traverse querySize q1
      plan <- maybe (throwAStr "oops") return
        =<< dropReader (fst <$> get) (getNodePlanFull ref)
      modify $ second $ refInsert ref $ Just ret
      return (ret,plan)) . refLU ref . snd
  where
    withPlan ts =
      maybe (throwAStr $ "Can't get plan:" ++ show ref) (return . (ts,))
      =<< dropState (gets fst,modify . first . const) (forceQueryPlan ref)
    getMedian = (>>= maybe (throwAStr "No size") return . medianSize)

medianOn :: Ord b => (a -> b) -> [a] -> Maybe a
medianOn _ [] = Nothing
medianOn f as = Just $ sortOn f as !! (length as `div` 2)

medianSize :: forall a . [(([TableSize],Double),a)]
           -> Maybe (([TableSize],Double),a)
medianSize qs = do
  qsWithSums :: [(PageNum,(([TableSize],Double),a))] <- forM qs
    $ \((tss,c),a) -> do
      pgss <- forM tss $ pageNum 4096
      return (sum pgss,((tss,c),a))
  (_sizeInt,ret) <- medianOn fst
    $ topPercOn 0.2 (snd . fst . snd) qsWithSums
  -- guard $ sizeInt < 50000
  return ret
  where

-- |Equivalent to (-) because the parser is a bit weird with the dash.
minus :: Num n => n -> n -> n
minus = (-)

topPercOn :: Double -> (a -> Double) -> [a] -> [a]
topPercOn pp f l = take n $ sortOn (minus 0 . f) l where
  n = max 1 $ round $ fromIntegral (length l) * pp

inferBinQuerySizeN :: Hashables2 e s =>
                     BQOp (PlanSym e s)
                   -> (([TableSize],Double),QueryPlan e s)
                   -> (([TableSize],Double),QueryPlan e s)
                   -> ([TableSize],Double)
inferBinQuerySizeN o ((lsize,lcert),lplan) ((rsize,rcert),rplan) =
  second (foldl (*) $ lcert * rcert)
  $ unzip
  $ zipWith (tableSizeComb combWidth combHeight) lsize rsize
  where
    (combWidth, combHeight :: Int -> Int -> (Int,Double)) = widthHeightOps o
    -- We assume that if one side selects unique columns from the
    -- other, that it will always be finding something.
    joinRows p = case (isUniqueSel lplan $ eqPairs p,isUniqueSel rplan $ eqPairs p) of
      (True,True)   -> cert 1 min
      (True,False)  -> cert 1 $ \ _ x -> x
      (False,True)  -> cert 1 const
      (False,False) -> cert 0.2 $ mul 0.70 ... (*)
    leftAntijoinRows p = if isUniqueSel lplan $ eqPairs p
      then cert 1 $ const $ const 0
      else cert 0.2 $ mul 0.1 ... const
    cert i = ((,i) ...)
    widthHeightOps = \case
      QProd            -> (cert 1 (+),cert 1 (*))
      -- We assume the smaller cardinality is always the lookup table
      QJoin p          -> (cert 1 (+),joinRows p)
      -- We assumen the smaller cardinality is always the lookup table
      QLeftAntijoin p  -> (cert 1 const,leftAntijoinRows p)
      QRightAntijoin p -> flip `bimap` flip $ widthHeightOps (QLeftAntijoin p)
      QDistinct        -> (cert 0.1 $ const id,cert 0.1 $ const id)
      QProjQuery       -> (cert 1 const,cert 1 $ const id)
      QUnion           -> (cert 1 const,cert 1 (+))

inferUnQuerySizeN :: UQOp e -> ([TableSize],Double) -> ([TableSize],Double)
inferUnQuerySizeN o (tbl,qcert) = second (foldl (*) qcert)
  $ unzip
  $ fmap
  (case o of
     QSel p -> if selectsSingleRow p
       then cert 1 $ tableSizeModify (const 1) id
       else cert 0.2 $ tableSizeModify (* 0.7) id
     -- XXX: we can know the projection width
     QProj _ -> cert 0.1 $ tableSizeModify (/ 2) id
     QSort _ -> cert 1 id
     QGroup _ [] -> cert 0.2 $ tableSizeModify (const 1) id
     -- XXX: we can know the projection width
     QGroup _ _ -> cert 0.1 $ tableSizeModify (* 0.5) id
     QLimit i -> cert 1 $ tableSizeModify (const $ fromIntegral i) id
     QDrop i -> cert 1 $ tableSizeModify (\x -> max 0 $ x - fromIntegral i) id)
  tbl
  where
    cert i = ((,i) .)
    selectsSingleRow _ = False
mul :: Double -> Int -> Int
mul theta = round . (* theta) . fromIntegral


cnfAnd :: forall x . Eq x => Prop x -> NEL.NonEmpty (Prop x)
cnfAnd = go where
  go x@(P0 _) = return x
  go x@(Not (P0 _)) = return x
  go (Not (Not x)) = go x
  go (Not (Or x y)) = cnfAnd (Not x) <> cnfAnd (Not y)
  go (Not (And x y)) = return $ Not (And x y)
  -- go (Not (And x y)) = cnfAnd $
  --   Or (foldr1Unsafe And $ cnfAnd $ Not x) (foldr1Unsafe And $ cnfAnd $ Not y)
  go (And x y) = cnfAnd x <> cnfAnd y
  go p@(Or x y) = fromMaybe (return p) $ goOr (cnfAnd x) (cnfAnd y)
    where
      goOr :: NEL.NonEmpty (Prop x)
           -> NEL.NonEmpty (Prop x)
           -> Maybe (NEL.NonEmpty (Prop x))
      goOr ls rs = NEL.nonEmpty $ inters ++ rest
        where
          rest = case (NEL.filter (`notElem` rs) ls,nonIntersR) of
            ([],[])       -> []
            (l,[])        -> l
            ([],r)        -> r
            (l:ls',r:rs') -> [Or (foldr And l ls') (foldr And r rs')]
          (inters,nonIntersR) = NEL.partition (`elem` ls) rs
  go _ = error "Unreachable, the price for using patterns."

eqPairs :: Eq e => Prop (Rel (Expr e)) -> [(Expr e,Expr e)]
eqPairs p = mapMaybe eqPair $ toList $ cnfAnd p
  where
    eqPair (P0 (R2 REq (R0 l) (R0 r))) = Just (l,r)
    eqPair _                           = Nothing
