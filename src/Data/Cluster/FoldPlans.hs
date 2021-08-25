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
import           Data.QnfQuery.Types
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
  => Proxy (ShapeSym e s)
  -> NodeRef n
  -> c NodeRef (ComposedType c (ShapeSym e s) NodeRef) t n
  -> [ClusterOp c (ShapeSym e s)]
clusterOp _ ref =
  -- Drop the ref part of the pairs
  concatMap fst
  -- We are only interested in the ones where ref is the one we are
  -- looking for
  . filter ((== ref) . snd)
  -- For each one get the (operator, ref) pair
  . fmap (extractRef (Proxy :: Proxy c) (Proxy :: Proxy (ShapeSym e s)))
  -- get the n-nodes
  . snd . allNodeRefs0


data Query1 e s = Q1' (UQOp e) s | Q2' (BQOp e) s s
  deriving (Functor,Traversable,Foldable,Generic)
instance Bifunctor Query1 where
  bimap f g
    = \case
        (Q1' o s)     -> Q1' (fmap f o) $ g s
        (Q2' o s1 s2) -> Q2' (fmap f o) (g s1) (g s2)

instance (AShow e,AShow s)
  => AShow (Query1 e s)
queryPlans1
  :: forall e s t n m err .
  (Hashables2 e s
  ,MonadState (ClusterConfig e s t n) m
  ,MonadAShowErr e s err m
  ,HasCallStack)
  => NodeRef n
  -> m [(NEL.NonEmpty (Query1 (ShapeSym e s) (NodeRef n)),AnyCluster e s t n)]

queryPlans1 refO = do
  clusts <- getClustersNonInput refO
  when (null clusts) $ do
    isInterm <- dropReader get $ isIntermediateClust refO
    allClusts <- lookupClustersN refO
    allQnfs <- lookupQnfN refO
    throwAStr
      $ "NodeRef is in none of the clusters: "
      ++ ashow (refO,fmap qnfOrigDEBUG' allQnfs,allClusts,isInterm)
  forM clusts $ \clust -> fmap (,clust) $ case clust of
    JoinClustW c -> getQueryRecurse2 (clusterInputs clust) c
    BinClustW c -> getQueryRecurse2 (clusterInputs clust) c
    UnClustW c -> getQueryRecurse1 (clusterInputs clust) c
    NClustW (NClust _) -> throwAStr "Looking for shapes for NClust.."
  where
    getQueryRecurse1
      :: [NodeRef n]
      -> UnClust e s t n
      -> m (NEL.NonEmpty (Query1 (ShapeSym e s) (NodeRef n)))
    getQueryRecurse1 inps c = case (inps,unOps) of
      ([inRef],Just ops) -> return $ (`Q1'` inRef) <$> ops
      (_,Nothing) -> do
        qnfs <- forM
          [snd $ unMetaD $ unClusterPrimaryOut c
          ,snd $ unMetaD $ unClusterSecondaryOut c
          ,refO]
          $ \x -> (x,) . qnfOrigDEBUG' . head <$> getNodeQnfN x
        throwAStr
          $ "No operators in cluster or clust does not contain the reference!!: "
          ++ ashow (refO,qnfs,c)
      _ -> inpNumError "UnClust" $ length inps
      where
        unOps = NEL.nonEmpty $ clusterOp (Proxy :: Proxy (ShapeSym e s)) refO c
    -- c is either a BinClust or JoinClust.
    getQueryRecurse2
      :: forall c' .
      (ClusterOp c' (ShapeSym e s) ~ BQOp (ShapeSym e s),SpecificCluster c')
      => [NodeRef n]
      -> c' NodeRef (ComposedType c' (ShapeSym e s) NodeRef) t n
      -> m (NEL.NonEmpty (Query1 (ShapeSym e s) (NodeRef n)))
    getQueryRecurse2 inps c = case (inps,binOps) of
      ([lref,rref],Just ops) -> return --
        $ (\o -> Q2' o lref rref) <$> ops
      (_,Nothing) -> throwAStr "No operators in cluster"
      _ -> inpNumError "BinClust or JoinClust" $ length inps
      where
        binOps =
          NEL.nonEmpty $ clusterOp (Proxy :: Proxy (ShapeSym e s)) refO c
    inpNumError :: String -> Int -> m x
    inpNumError s n =
      error $ printf "clusterInpts returns %d elements over %s" n s

querySize1
  :: forall e s t n m .
  (Hashables2 e s
  ,MonadError (SizeInferenceError e s t n) m
   -- Nothing in a node means we are currently looking up the node
  ,MonadState (ClusterConfig e s t n,RefMap n (Maybe QueryShape)) m)
  => AnyCluster e s t n
  -> Query1 (ShapeSym e s) (QueryShape e s)
  -> m ([TableSize],Double)
querySize1 clust q = do
  ((_,shapeClust) <- dropReader (gets fst)
    $ getValidClustPropagator clust
  dropState (gets fst,modify . first . const) $ putShapeCluster shapeClust
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
           -> [(ShapeSym e s,ShapeSym e s)]
           -> (([TableSize],Double),QueryShape e s)
           -> m (([TableSize],Double),QueryShape e s)
    transl o assoc shape =
      either (const $ throwAStr $ "Op: " ++ ashow (shape,assoc)) return
      $ traverse (translateShapeMap'' (chooseSym o) assoc) shape

querySize
  :: forall e s t n m .
  (Hashables2 e s
  ,MonadError (SizeInferenceError e s t n) m
   -- Nothing in a node means we are currently looking up the node
  ,MonadState (ClusterConfig e s t n,RefMap n (Maybe ([TableSize],Double))) m)
  => NodeRef n
  -> m (([TableSize],Double),QueryShape e s)
querySize ref = get >>= go . refLU ref . snd
  where
    go = \case
      Just (Just x) -> withShape x
      Just Nothing -> do
        cs :: [(NEL.NonEmpty (Query1 (ShapeSym e s) (NodeRef n))
               ,AnyCluster e s t n)] <- dropState
          (gets fst,const $ return ())
          (queryPlans1 ref)
        -- Note: in the clusters appearing here `ref` is a non-input node.
        throwAStr $ "Cycle: " ++ ashow (ref,cs)
      Nothing
       -> dropState (gets fst,const $ return ()) (queryPlans1 ref) >>= \case
        [] -> throwAStr $ "Size of bot node should be in cache: " ++ show ref
        clusts -> getMedian $ takeListT 100 $ do
          (q1 NEL.:| _,clust) <- mkListT $ return clusts -- certainly non empty
          modify $ second $ refInsert ref Nothing
          -- querySize1 also triggers the propagator for the plans.
          ret <- querySize1 clust =<< traverse querySize q1
          plan <- maybe (throwAStr "oops") return
            =<< dropReader (gets fst) (getNodeShapeFull ref)
          modify $ second $ refInsert ref $ Just ret
          return (ret,plan)
    withShape ts =
      maybe (throwAStr $ "Can't get plan:" ++ show ref) (return . (ts,))
      =<< dropState (gets fst,modify . first . const) (forceQueryShape ref)
    getMedian xs = do
      lst <- xs
      case medianSize lst of
        Nothing -> throwAStr
          $ printf
            "No candidate size for %s (candidates: %d)"
            (ashow ref)
            (length lst)
        Just x -> return x


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

-- |Equivalent to (-) because the parser is a bit weird with the dash.
minus :: Num n => n -> n -> n
minus = (-)

topPercOn :: Double -> (a -> Double) -> [a] -> [a]
topPercOn pp f l = take n $ sortOn (minus 0 . f) l where
  n = max 1 $ round $ fromIntegral (length l) * pp

inferBinQuerySizeN
  :: Hashables2 e s
  => BQOp (ShapeSym e s)
  -> QueryShape e s
  -> QueryShape e s
  -> QuerySize e s
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

inferUnQuerySizeN :: UQOp e -> QuerySize -> QuerySize
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


qnfAnd :: forall x . Eq x => Prop x -> NEL.NonEmpty (Prop x)
qnfAnd = go where
  go x@(P0 _) = return x
  go x@(Not (P0 _)) = return x
  go (Not (Not x)) = go x
  go (Not (Or x y)) = qnfAnd (Not x) <> qnfAnd (Not y)
  go (Not (And x y)) = return $ Not (And x y)
  -- go (Not (And x y)) = qnfAnd $
  --   Or (foldr1Unsafe And $ qnfAnd $ Not x) (foldr1Unsafe And $ qnfAnd $ Not y)
  go (And x y) = qnfAnd x <> qnfAnd y
  go p@(Or x y) = fromMaybe (return p) $ goOr (qnfAnd x) (qnfAnd y)
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
eqPairs p = mapMaybe eqPair $ toList $ qnfAnd p
  where
    eqPair (P0 (R2 REq (R0 l) (R0 r))) = Just (l,r)
    eqPair _                           = Nothing
