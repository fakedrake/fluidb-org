{-# LANGUAGE CPP                       #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE LambdaCase                #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TupleSections             #-}
{-# LANGUAGE TypeFamilies              #-}

-- | Normalize queries for comparison

module Data.QnfQuery.Build
  (toNQNFQuery
  ,toNQNFQueryU
  ,toNQNFQueryB
  ,nqnfJoin
  ,nqnfLeftAntijoin
  ,nqnfRightAntijoin
  ,toQNF
  ,getName
  ,getNameTrusting
  ,nqnfSymbol
  ,putIdentityQNFQ
  ,putEmptyQNFQ
  ,MkQB(..)
  ,listTMaxQNF
  ,toNQNF) where

import           Control.Monad.Except
import           Data.Bifunctor
import           Data.Functor.Identity
import qualified Data.HashMap.Lazy          as HM
import qualified Data.HashSet               as HS
import           Data.List.Extra
import qualified Data.List.NonEmpty         as NEL
import           Data.Maybe
import           Data.QnfQuery.BuildProduct
import           Data.QnfQuery.BuildUtils
import           Data.QnfQuery.HashBag
import           Data.QnfQuery.SanityCheck
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Utils.AShow
import           Data.Utils.Compose
import           Data.Utils.Const
import           Data.Utils.EmptyF
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.Tup
import           Data.Utils.Unsafe


-- | Turn a query into QNF and map the symbols to the QNFNames. We use
-- integers to disambiguate between symbols that refer to columns that
-- have the same data so we return all possible combinations.
toQNF :: forall e s .
      (HasCallStack,Hashables2 e s)
      => (s -> Maybe [e])
      -> Query e s
      -> ListT (QNFBuild e s) (QNFQuery e s,Query (QNFName e s,e) s)
toQNF allSyms query = first snd <$> toNQNF allSyms query

toNQNF
  :: forall e s .
  (HasCallStack,Hashables2 e s)
  => (s -> Maybe [e])
  -> Query e s
  -> ListT (QNFBuild e s) (NQNFQuery e s,Query (QNFName e s,e) s)
toNQNF allSyms query = do
  NQNFResult{..} <- toNQNFQuery =<< traverse putNQNF query
  return (putQNQNF (Identity query) nqnfResNQNF,nqnfResOrig)
  where
    putNQNF :: s -> ListT (QNFBuild e s) (NQNFQueryI e s, s)
    putNQNF s = do
      es <- maybe (throwAStr $ "Can't get symbols of " ++ ashow s) return
           $ allSyms s
      return (putQNQNF EmptyF $ nqnfSymbol es s,s)

putQNQNF :: g (Query e s)
         -> NQNFQueryDCF d c f e s
         -> NQNFQueryDCF g c f e s
putQNQNF q = fmap $ putQQNF q

putQQNF :: g (Query e s)
        -> QNFQueryDCF d c f e s
        -> QNFQueryDCF g c f e s
putQQNF q qnf = qnf{qnfOrigDEBUG'=q}


putEmptyQNFQ :: QNFQueryDCF d c f e s
             -> QNFQueryDCF EmptyF c f e s
putEmptyQNFQ qnf = qnf{qnfOrigDEBUG'=EmptyF}
putIdentityQNFQ :: Query e s
                -> QNFQueryDCF d c f e s
                -> QNFQueryDCF Identity c f e s
putIdentityQNFQ q qnf = qnf{qnfOrigDEBUG'=Identity q}


mapOp :: (op -> op') -> NQNFResultDF d f op e s -> NQNFResultDF d f op' e s
mapOp fop NQNFResult{..} = NQNFResult {
  nqnfResNQNF=nqnfResNQNF,
  nqnfResOrig=fop nqnfResOrig,
  nqnfResInOutNames=nqnfResInOutNames
  }
rebaseOp :: Hashables2 e s =>
           NQNFResultI (UQOp (QNFName e s, e)) e s
         -> Either (QNFError e s) (NQNFResultI (UQOp (QNFName e s, e)) e s)
rebaseOp res = do
  let translSym = \case
        (Column col 0,e) -> case lookup (e,col) $ nqnfResInOutNames res of
          Nothing        -> throwAStr "oops"
          Just (e',col') -> return (Column col' 0,e')
        (Column _ _,_) -> throwAStr "oops"
        (PrimaryCol {},_) -> throwAStr
          $ "Since we are refering to output we can't have"
          ++ " primary columns in op since at the very least the op itself "
          ++ " is included in the op."
        x -> return x
  op' <- traverse translSym $ nqnfResOrig res
  return
    res { nqnfResOrig = op'
        }

toNQNFQueryU :: forall e s . (HasCallStack, Hashables2 e s) =>
               UQOp e
             -> NQNFQueryI e s
             -> QNFBuild e s (NQNFResultI (UQOp (QNFName e s, e)) e s)
toNQNFQueryU o nqnfIn@(nmIn,_) = do
  -- Use input namemap
  let o' = (\x -> (getNameTrusting nmIn x,x)) <$> o
  let putO :: NQNFResultI a e s -> NQNFResultI (UQOp (QNFName e s, e)) e s
      putO = mapOp (const o')
  (sanityCheckRes ("UOP: " ++ ashow o) id =<<) $ case o of
  -- case o of
    QSel p     -> mapOp QSel <$> nqnfSelect p nqnfIn
    QProj p    -> mapOp QProj <$> nqnfProject p nqnfIn
    QGroup p e -> mapOp (uncurry QGroup) <$> nqnfAggregate p e nqnfIn
    QSort _    -> lift $ rebaseOp $ putO $ nqnfAny1 (fst <$> o') nqnfIn
    QLimit _   -> return $ putO $ nqnfAny1 (fst <$> o') nqnfIn
    QDrop _    -> return $ putO $ nqnfAny1 (fst <$> o') nqnfIn

newtype MkQB e s = MkQB { mkQB :: forall a . a -> a -> Query (QNFName e s,e) a}


-- This is meant to be used like this. But it has to be done ONCE, at
-- the END. Otherwise the order of the products may prodice different
-- interediate maximum hashes. If everything works as expected the
-- list will be the same in different order.
--
-- listTMaxQNF (nqnfResNQNF)
listTMaxQNF
  :: (Hashables2 e s,Monad m)
  => (a -> QNFQueryDCF d Either HashBag e s)
  -> ListT m a
  -> m (Maybe a)
listTMaxQNF f l = return . maximumOn (hash . f) <$> runListT l

toNQNFQueryB
  :: forall e s .
  (HasCallStack,Hashables2 e s)
  => BQOp e
  -> NQNFQueryI e s
  -> NQNFQueryI e s
  -> ListT
    (QNFBuild e s)
    (NQNFResultI (Either (BQOp (QNFName e s,e)) (MkQB e s)) e s)
toNQNFQueryB o l r =
  (sanityCheckRes ("BOP: " ++ ashow o) (Compose . either Just (const Nothing)) =<<)
  $ case o of
    QJoin p -> mapOp' QJoin <$> nqnfJoin p l r
    QProd -> do
      ret <- mapOp' (const QProd) <$> nqnfProduct l r
      when (bagNull $ qnfProd $ snd r)
        $ throwAStr
        $ "L Same in as out. R Cols: "
        ++ ashow (qnfColumns $ snd r)
      return ret
    QUnion -> lift2 $ mapOp' (const QUnion) <$> nqnfUnion l r
    -- Note: Antijoins have unambiguous outputs but the operator may
    -- have conflicting symbols. We solve the problem of symbol
    -- conflict in joins so we inherit the work from there to
    -- disambiguate the symbols in op by disambiguation but the
    -- columns are not incremented.
    QLeftAntijoin p -> do
      p' <- nqnfResOrig <$> nqnfJoin p l r
      return $ mapOp' (const $ QLeftAntijoin p') $ nqnfLeftAntijoin p' l r
    QRightAntijoin p -> do
      p' <- nqnfResOrig <$> nqnfJoin p l r
      return $ mapOp' (const $ QRightAntijoin p') $ nqnfRightAntijoin p' l r
    QProjQuery -> lift $ do
      keyAssoc <- leftAsRightKeys
      mapOp (\p -> Right $ MkQB $ \_ q -> Q1 (QProj p) (Q0 q))
        <$> nqnfProject [(kl,E0 kr) | (kl,kr) <- keyAssoc] r
    -- All columns in the left appear on the right so just keep the
    -- columns of th right
    QDistinct -> lift $ do
      keyAssoc <- leftAsRightKeys
      mapOp (\(p,e) -> Right $ MkQB $ \_ q -> Q1 (QGroup p e) (Q0 q))
        <$> nqnfAggregate
          [(kl,E0 $ NAggr AggrFirst $ E0 kr) | (kl,kr) <- keyAssoc]
          (E0 . snd <$> keyAssoc)
          r
  where
    -- In QDistinct and QProjQuery if l has automatically exposed
    -- unique keys they will have necessarily different names to the
    -- corresponding r uniquekeys. Keep the r version of each key to
    -- preserve the name.
    --
    -- This function finds the keys on left that are also on r. This is
    leftAsRightKeys :: QNFBuild e s [(e,e)]
    leftAsRightKeys = traverse findColOnR $ HM.toList $ nameMap l
      where
        findColOnR :: (e,QNFCol e s) -> QNFBuild e s (e,e)
        findColOnR lcol =
          maybe
            (throwAStr "Col in lhs not found in rhs")
            (return . (fst lcol,) . fst)
          $ find (equivCol lcol)
          $ HM.toList
          $ fst r
          where
            equivCol :: (e,QNFCol e s) -> (e,QNFCol e s) -> Bool
            equivCol (el,cl) (er,cr) =
              el == er || qnfColumns cl == qnfColumns cr
    mapOp' :: (op -> BQOp (QNFName e s,e))
           -> NQNFResultDF d f op e s
           -> NQNFResultDF d f (Either (BQOp (QNFName e s,e)) (MkQB e s)) e s
    mapOp' opf = mapOp $ Left . opf

nqnfLeftAntijoin :: Hashables2 e s =>
                   Prop (Rel (Expr (QNFName e s, e)))
                  -> NQNFQueryI e s
                  -> NQNFQueryI e s
                  -> NQNFResultI () e s
nqnfLeftAntijoin p' l r = mapOp (const ()) $ nqnfAny2 const (lso,l,r) [(rso,r,l)]
  where
    lso = QLeftAntijoin p'
    rso = QRightAntijoin p'

nqnfRightAntijoin :: Hashables2 e s =>
                    Prop (Rel (Expr (QNFName e s, e)))
                  -> NQNFQueryI e s
                  -> NQNFQueryI e s
                  -> NQNFResultI () e s
nqnfRightAntijoin p' l r =
  mapOp (const ()) $ nqnfAny2 (const id) (lso,l,r) [(rso,r,l)]
  where
    lso = QRightAntijoin p'
    rso = QLeftAntijoin p'

toNQNFQuery
  :: forall e s s0 .
  (HasCallStack,Hashables3 e s s0)
  => Query e (NQNFQueryI e s,s0)
  -> ListT (QNFBuild e s) (NQNFResultI (Query (QNFName e s,e) s0) e s)
toNQNFQuery = recur
  where
    recur = \case
      Q2 o l r -> do
        Tup2 resl resr <- recur `traverse` Tup2 l r
        mapOp
          (either
             (\o' -> Q2 o' (nqnfResOrig resl) (nqnfResOrig resr))
             (\(MkQB f) -> join $ f (nqnfResOrig resl) (nqnfResOrig resr)))
          <$> toNQNFQueryB o (nqnfResNQNF resl) (nqnfResNQNF resr)
      Q1 o nqnf -> do
        res <- recur nqnf
        lift
          $ mapOp (`Q1` nqnfResOrig res)
          <$> toNQNFQueryU o (nqnfResNQNF res)
      Q0 (s,s0) -> return
        NQNFResult
        { nqnfResNQNF = s,nqnfResOrig = Q0 s0,nqnfResInOutNames = [] }


nqnfSymbol :: forall e s. HashableQNF HashBag e s =>
             [e] -> s -> NQNFQuery e s
nqnfSymbol es s = do
    (HM.fromList $ fmap2 (putQQNF EmptyF) colAssoc, qnf)
  where
    colAssoc = [(e,qnfSymbol $ Identity $ E0 $ PrimaryCol e s 0) | e <- es]
    qnf = qnfSymbol $ toHashBag [E0 $ PrimaryCol e s 0 | e <- es]
    qnfSymbol :: HashableQNF f e s =>
                QNFProj f e s -> QNFQueryDCF Identity Either f e s
    qnfSymbol cols = updateHash QNFQuery {
      qnfColumns=Left cols,
      qnfSel=mempty,
      qnfHash=undefined,
      qnfProd=bagSingleton $ HS.singleton $ Q0 $ Left s,
      qnfOrigDEBUG'=Identity $ Q0 s}


-- | If all of namemaps are E0 return the aggr otherwise wrap in
-- projections.
nqnfCombineCols :: forall e s .
                  (Hashables2 e s, HashableQNF Identity e s) =>
                  [(e,QNFColProj e s)] -> NQNFQueryI e s
nqnfCombineCols colAssocC = (nm,qnf) where
  colGAssoc :: [(e,Expr (QNFName e s))]
  colGAssoc = [(e,runIdentity $ getConst $ qnfColumns col)
              | (e,col) <- colAssocC]
  commonQnf = qnfDropColumnsF $ snd $ headErr colAssocC
  mkQnf :: HashableQNFC c' f e s =>
          c' (QNFProj f e s) (QNFAggr f e s)
        -> QNFQueryCF c' f e s
  mkQnf a = updateHash QNFQuery {
    qnfColumns=a,
    qnfSel=qnfSel commonQnf,
    qnfProd=qnfProd commonQnf,
    qnfHash=undefined,
    qnfOrigDEBUG'=EmptyF}
  colAssoc :: [(e,QNFCol e s)] = fmap2 (mkQnf . Left . Identity) colGAssoc
  nm :: NameMap e s
  nm = HM.fromList colAssoc
  qnf :: QNFQueryI e s
  qnf = mkQnf $ Left $ toHashBag $ snd <$> colGAssoc

-- |Make a result record for a projection.
nqnfProject :: forall e e' s .
              (e' ~ (QNFName e s,e), Hashables2 e s) =>
              [(e,Expr e)]
            -> NQNFQueryI e s
            -> QNFBuild e s (NQNFResultI [(e',Expr e')] e s)
nqnfProject p l =
  qnfCached qnfProject_cache (\cc x -> cc{qnfProject_cache=x}) (p,l)
  $ nqnfProjectI p l
nqnfProjectI :: forall e e' s .
               (e' ~ (QNFName e s,e), Hashables2 e s) =>
               [(e,Expr e)]
             -> NQNFQueryI e s
             -> Either (QNFError e s) (NQNFResultI [(e',Expr e')] e s)
nqnfProjectI [] _ = return NQNFResult{
  nqnfResInOutNames=[],
  nqnfResOrig=[],
  nqnfResNQNF=(mempty,
               updateHash QNFQuery{
                  qnfColumns=Left mempty,
                  qnfProd=mempty,
                  qnfSel=mempty,
                  qnfOrigDEBUG'=EmptyF,
                  qnfHash=undefined})}
nqnfProjectI proj nqnfIn = do
  colsOutAssoc0 :: [(e, (QNFColProj e s, Expr e'))] <-
    nqnfProjCol nqnfIn `traverse2` proj
  let colsOutAssoc = colsOutAssoc0
  let nqnf = nqnfCombineCols $ fmap2 fst colsOutAssoc
  return $ mkRes colsOutAssoc nqnf
  where
    mkRes :: [(e, (QNFColProj e s,Expr e'))]
          -> NQNFQueryI e s
          -> NQNFResultI [(e', Expr e')] e s
    mkRes colsAssoc nqnfOut = NQNFResult {
      nqnfResNQNF=nqnfOut,
      nqnfResOrig=[((Column (qnfGeneralizeQueryF $ Left outCol) 0,e), expr)
                  | (e,(outCol,expr)) <- colsAssoc],
      -- There is no one-to-one correspondance of in/out names in
      -- aggregations.
      nqnfResInOutNames=[]
      }

nqnfAggregate
  :: forall e s e' .
  (e' ~ (QNFName e s,e),Hashables2 e s)
  => [(e,Expr (Aggr (Expr e)))]
  -> [Expr e]
  -> NQNFQueryI e s
  -> QNFBuild e s (NQNFResultI ([(e',Expr (Aggr (Expr e')))],[Expr e']) e s)
nqnfAggregate p g l =
  qnfCached qnfAggregate_cache (\cc x -> cc{qnfAggregate_cache=x}) (p,g,l)
  $ nqnfAggregateI p g l

nqnfAggregateI
  :: forall e s e' .
  (e' ~ (QNFName e s,e),Hashables2 e s)
  => [(e,Expr (Aggr (Expr e)))]
  -> [Expr e]
  -> NQNFQueryI e s
  -> Either
    (QNFError e s)
    (NQNFResultI ([(e',Expr (Aggr (Expr e')))],[Expr e']) e s)
nqnfAggregateI proj aggrBases0 nqnfIn@(nmIn,_) = do
  prunnedProj :: [(e,Expr (Aggr (QNFColProj e s,Expr (QNFName e s,e))))]
    <- traverse4 (nqnfProjCol nqnfIn) proj
  aggrBases :: [(QNFColProj e s,Expr (QNFName e s,e))]
    <- traverse (nqnfProjCol nqnfIn) aggrBases0
  -- Create an inner projection. XXX: check tha it's a noop.
  let projQnfInner :: QNFQueryProj e s =
        qnfConcatProjColsUnsafe $ fmap fst $ aggrBases ++ toList4 prunnedProj
  -- Transform the expression subtrees of the aggregation to become
  -- projection columns. XXX: If projQnfInner is not the same as nqnfIn
  let aggrCols :: [(e,Expr (QNFColAggr e s))] =
        fmap3
          (qnfAggrCol (projQnfInner,fst <$> aggrBases) . fmap fst)
          prunnedProj
  -- Concatenate the aggregation columns to become a qnf.
  let aggrQnf :: QNFQueryAggr e s = qnfConcatAggrColsUnsafe $ toList3 aggrCols
  -- If the aggregation columns are trivial (ie. E0 (Aggr...) rather
  -- than more complex expressions). Then don't wrap
  let (nmOut,qnfOut) = case trvialColumns aggrCols of
        Just projAssoc
          -> let nmOut0 =
                   HM.fromList $ fmap2 (qnfGeneralizeQueryF . Right) projAssoc
                     :: NameMap e s
                 qnfOut0 =
                   qnfGeneralizeQueryF
                   $ Right
                   $ qnfConcatAggrColsUnsafe
                   $ toList2 projAssoc in (nmOut0,qnfOut0)
        Nothing
         -> let nameAssoc =
                  fmap2 (aggrColToProjCol aggrQnf) aggrCols
                    :: [(e,QNFColProj e s)]
                nmOut0 =
                  HM.fromList $ fmap2 (qnfGeneralizeQueryF . Left) nameAssoc
                    :: NameMap e s
                qnfOut0 =
                  qnfGeneralizeQueryF
                  $ Left
                  $ qnfConcatProjColsUnsafe
                  $ toList2 nameAssoc in (nmOut0,qnfOut0)
  return
    NQNFResult
    { nqnfResNQNF = (nmOut,qnfOut)
     ,nqnfResOrig =
        (bimap
           (\e -> (getNameTrusting nmOut e,e))
           (fmap3 $ \e -> (getNameTrusting nmIn e,e))
         <$> proj
        ,fmap2 (\e -> (getNameTrusting nmIn e,e)) aggrBases0)
      -- There is no one-to-one correspondance of in/out names for
      -- aggregations
     ,nqnfResInOutNames = []
    }

-- | If the columns are all trivial (ie. all are E0) strip the Expr
-- layer.
trvialColumns :: [(e, Expr (QNFColAggr e s))] -> Maybe [(e, QNFColAggr e s)]
trvialColumns = foldr go (Just []) where
  go (n,E0 a) rest = ((n,a):) <$> rest
  go _ _           = Nothing

-- | Concatenate the columns into a single QNF assuming that they are
-- all columns of the same QNF.
qnfConcatProjColsUnsafe :: forall e s. Hashables2 e s =>
                          [QNFColProj e s] -> QNFQueryProj e s
qnfConcatProjColsUnsafe [] = updateHash QNFQuery {
  qnfColumns=Const mempty,
  qnfSel=mempty,
  qnfProd=mempty,
  qnfHash=undefined,
  qnfOrigDEBUG'=EmptyF}
qnfConcatProjColsUnsafe proj@(p:_) = updateHash QNFQuery {
  qnfColumns=Const $ toHashBag $ runIdentity . getConst . qnfColumns <$> proj,
  qnfSel=qnfSel p,
  qnfProd=qnfProd p,
  qnfHash=undefined,
  qnfOrigDEBUG'=EmptyF}
-- | Concatenate the columns into a single QNF assuming that they are
-- all columns of the same QNF.
qnfConcatAggrColsUnsafe
  :: forall e s . Hashables2 e s => [QNFColAggr e s] -> QNFQueryAggr e s
qnfConcatAggrColsUnsafe [] = updateHash QNFQuery {
  qnfColumns=CoConst mempty,
  qnfSel=mempty,
  qnfProd=mempty,
  qnfHash=undefined,
  qnfOrigDEBUG'=EmptyF}
qnfConcatAggrColsUnsafe proj@(p:_) = updateHash QNFQuery {
  qnfColumns=CoConst
    (toHashBag $ fmap (runIdentity . fst) $ getCoConst . qnfColumns <$> proj,
     snd $ getCoConst $ qnfColumns p),
  qnfSel=qnfSel p,
  qnfProd=qnfProd p,
  qnfHash=undefined,
  qnfOrigDEBUG'=EmptyF}

qnfAggrCol :: Hashables2 e s =>
             (QNFQueryProj e s,[QNFColProj e s])
           -> Aggr (QNFColProj e s)
           -> QNFColAggr e s
qnfAggrCol (innerQ,exps) aggr = updateHash QNFQuery {
  qnfColumns=CoConst
    (Identity $ (`Column` 0) . qnfGeneralizeQueryF . Left <$> aggr,
      -- Identity $ runIdentity . getConst . qnfColumns <$> aggr,
     HS.fromList $ E0 . (`Column` 0) . qnfGeneralizeQueryF . Left <$> exps),
  qnfSel=mempty,
  qnfProd=bagSingleton $
    HS.singleton $ Q0 $ Right $ qnfGeneralizeQueryF $ Left innerQ,
  qnfHash=error "Uncomputed hash:qnfAggrCol",
  qnfOrigDEBUG'=EmptyF}


-- | Restrict a query to become a projection column.
nqnfProjCol
  :: forall e s .
  Hashables2 e s
  => NQNFQueryI e s
  -> Expr e
  -> Either (QNFError e s) (QNFColProj e s,Expr (QNFName e s,e))
nqnfProjCol nqnf projExp = nqnfSpecializeQueryF nqnf >>= \case
  Left (projNm,projQnf)
    -> let qnf =
             qnfMapColumns
               (const
                $ Const
                $ Identity
                $ projExp >>= generalizeProjName . getNameTrusting projNm)
               projQnf in return (qnf,exprPair)
  Right (aggrNm,aggrQnf) -> return
    (updateHash
       QNFQuery
       { qnfColumns = Const
           $ Identity
           $ generalizeAggrName . getNameTrusting aggrNm <$> projExp
        ,qnfSel = mempty
        ,qnfHash = error "Uncomputed hash:nqnfProjCol"
        ,qnfProd = bagSingleton
           $ HS.singleton
           $ Q0
           $ Right
           $ qnfGeneralizeQueryF
           $ Right aggrQnf
        ,qnfOrigDEBUG' = EmptyF
       }
    ,exprPair)
  where
    exprPair :: Expr (QNFName e s,e)
    exprPair = go <$> projExp
      where
        go :: e -> (QNFName e s,e)
        go e = (getNameTrusting (fst nqnf) e,e)
    -- | Wrap the name
    generalizeAggrName :: QNFNameC CoConst e s -> QNFName e s
    generalizeAggrName = qnfMapName $ Right . getCoConst
    -- | If the name is a column expand the contents.
    generalizeProjName :: QNFNameC Const e s -> Expr (QNFName e s)
    generalizeProjName = \case
      Column QNFQuery {qnfColumns = Const (Identity expr)} _ -> expr
      PrimaryCol e s i -> E0 $ PrimaryCol e s i
      NonSymbolName e -> E0 $ NonSymbolName e


nqnfAny1 :: forall e s . Hashables2 e s =>
           UQOp (QNFName e s)
         -> NQNFQueryI e s
         -> NQNFResultI () e s
nqnfAny1 o (nm, qnf) = NQNFResult {
  nqnfResNQNF=(nmRet,qnfRet),
  nqnfResOrig=(),
  nqnfResInOutNames=[((e,inC),(e,outC)) | ((e,inC),outC) <- triplets]
  }
  where
    qnfRet :: QNFQueryI e s
    qnfRet = wrapOuterQnf
      $ Left $ toHashBag [E0  $ Column c 0 | ((_,c),_) <- triplets]
    nmRet :: NameMap e s
    nmRet = HM.fromList [(e,outC) | ((e,_),outC) <- triplets]
    wrapOuterQnf :: HashableQNFC c f e s =>
                   c (QNFProj f e s) (QNFAggr f e s)
                 -> QNFQueryDCF EmptyF c f e s
    wrapOuterQnf p = updateHash QNFQuery {
      qnfColumns=p,
      qnfSel=mempty,
      qnfHash=undefined,
      qnfProd=qnfProdSingleton $ Q1 o $ Q0 $ Right qnf,
      qnfOrigDEBUG'=EmptyF}
    triplets :: [((e, QNFCol e s), QNFCol e s)]
    triplets = [((e,inC),wrapOuterQnf $ Left $ Identity $ E0 $ Column inC 0)
               | (e,inC) <- HM.toList nm]


-- | Given a qnf that contains a col `in` qnfProduct(qnf) get the top
-- level column. eg
--
--   toOutCol QNF[_|_ , S, [{Q0 <C1,C2,C3}]] C2
--   ==> QNF[C2 , S, [{Q0 <C1,C2,C3>}]]
toOutColumnUnsafe :: HashableQNF f e s =>
                    QNFQueryF f e s -> QNFCol e s -> QNFCol e s
toOutColumnUnsafe outQnf col =
  runIdentity
  $ qnfTravColumns (const $ Identity $ Left $ Identity $ E0 $ Column col 0)
  outQnf

-- Make an NQNF with alternative represnetations. Select just cooses
-- either nml or nmr.
nqnfAny2 :: forall e s . Hashables2 e s =>
           (forall a . a -> a -> a)
         -> (BQOp (QNFName e s,e), NQNFQueryI e s, NQNFQueryI e s)
         -> [(BQOp (QNFName e s,e), NQNFQueryI e s, NQNFQueryI e s)]
         -> NQNFResultI (BQOp (QNFName e s,e)) e s
nqnfAny2 select q0@(op,(nml, _),(nmr, _)) qs = NQNFResult {
  nqnfResNQNF=(nm, outQnf),
  nqnfResOrig=op,
  nqnfResInOutNames=[(c,toOutColumnUnsafe outQnf <$> c)
                    | c <- HM.toList $ select nml nmr]
  }
  where
    outQnf :: QNFQueryI e s
    outQnf = wrapOuterQnf $ Left $ toHashBag
             [ E0 $ Column c 0 | c <- HM.elems $ select nml nmr]
    nm :: NameMap e s
    nm = toOutColumnUnsafe outQnf <$> select nml nmr
    wrapOuterQnf p = updateHash QNFQuery {
      qnfColumns=p,
      qnfSel=mempty,
      qnfHash=undefined,
      qnfProd=qnfProdSingleton2
        [(fst <$> o,q,q')
        | (o,(_,q),(_,q')) <- q0:qs],
      qnfOrigDEBUG'=EmptyF}

-- |The columns of the qnf are the columns of the first argument.
nqnfUnion :: forall e s . (Hashables2 e s) =>
            NQNFQueryI e s
          -> NQNFQueryI e s
          -> Either (QNFError e s) (NQNFResultI () e s)
nqnfUnion = go
  where
    go :: NQNFQueryI e s
       -> NQNFQueryI e s
       -> Either (QNFError e s) (NQNFResultI () e s)
    go (nm, qnf) (nm', qnf') = do
      -- XXX: Assert that keys are actually a subset.
      when (HM.keys nm' `subset` HM.keys nm)
        $ throwError $ QNFSchemaMismatch (HM.keys nm) (HM.keys nm')
      let outCols :: [(e,QNFCol e s)] =
            [(e,toOutColumnUnsafe (qnfRet (\ _ _ -> EmptyF) EmptyF) c)
            | (e,c) <- HM.toList nm]
      return NQNFResult {
        nqnfResNQNF=(HM.fromList outCols,
                     qnfRet bagMap $ toHashBag $ snd <$> outCols),
        nqnfResOrig=(),
        nqnfResInOutNames=do
            -- nm is a superset of nm' so c will always be in nm
            (e,cPreOut NEL.:| cs) <-
              HM.toList $ HM.unionWith (<>) (return <$> nm) (return <$> nm')
            cIn <- cPreOut:cs
            return ((e,cIn),(e,qnfRet fmap $ Identity cPreOut))
        }
      where
        subset xs = all (`elem` xs)
        qnfRet :: HashableQNF f' e s =>
                 ((QNFColC c e1 s1 -> Expr (QNFNameC c e1 s1)) ->
                  f (QNFCol e s) -> QNFProj f' e s)
               -> f (QNFCol e s) -> QNFQueryF f' e s
        qnfRet fmap' colIn = updateHash $ qnf{
          qnfColumns=Left $ fmap' (E0 . (`Column` 0)) colIn,
          qnfSel=mempty,
          qnfProd=qnfProdSingleton $ Q2 QUnion (Q0 $ Right qnf) (Q0 $ Right qnf')}

nqnfJoin :: Hashables2 e s =>
           Prop (Rel (Expr e))
         -> NQNFQueryI e s
         -> NQNFQueryI e s
         -> ListT (QNFBuild e s)
         (NQNFResultI (Prop (Rel (Expr (QNFNameC Either e s, e)))) e s)
nqnfJoin p l r = do
  resProd <- nqnfProduct l r
  resSel <- lift $ nqnfSelect p $ nqnfResNQNF resProd
  case nqnfResInOutNames resProd `chainAssoc` nqnfResInOutNames resSel of
    Nothing    -> throwAStr "Prod/sel symbol translation in/out maps don't match"
    Just finIO -> return resSel{nqnfResInOutNames=finIO}

nqnfSelect
  :: forall e s .
  Hashables2 e s
  => Prop (Rel (Expr e))
  -> NQNFQueryI e s
  -> QNFBuild e s (NQNFResultI (Prop (Rel (Expr (QNFName e s,e)))) e s)
nqnfSelect p l =
  qnfCached qnfSelect_cache (\cc x -> cc{qnfSelect_cache=x}) (p,l)
  $ nqnfSelectI p l

nqnfSelectI
  :: forall e s .
  Hashables2 e s
  => Prop (Rel (Expr e))
  -> NQNFQueryI e s
  -> Either
    (QNFError e s)
    (NQNFResultI (Prop (Rel (Expr (QNFName e s,e)))) e s)
nqnfSelectI p q = nqnfSpecializeQueryF q <&> \case
  Left proj  -> putName $ selInternal proj
  Right aggr -> putName $ selInternal $ aggrToProjI aggr
  where
    aggrToProjI = runIdentity . nqnfLiftC1 (Identity . nqnfAggrAsProj)
    selInternal = nqnfSelectInternal (Identity p)
    putName = mapOp $ fmap3 (first $ qnfMapName $ Left . getConst) . runIdentity

-- Selection refers to the output columns (obviously since we have no
-- other way of accessing the input columns). This means that the
-- correct implementation is cyclical. Therefore we drop the selection
-- and product info aspect from all symbols in qnfSel. It is obvious
-- that they refer to the product and selection if the query itself.
--
-- Also selection is under the proj/aggr operation. Therefore if we
-- are inserting into an aggregated query we need to first wrap it
-- into a projection.
--
-- Nothing in prop means drop selection from query.
nqnfSelectInternal
  :: forall dr f e s .
  (Functor dr,HashableQNF f e s,Foldable dr)
  => dr (Prop (Rel (Expr e)))
  -> NQNFQueryCF Const f e s
  -> NQNFResultF f (dr (Prop (Rel (Expr (QNFNameC Const e s,e))))) e s
nqnfSelectInternal pM (nm,qnf0) =
  NQNFResult
  { nqnfResNQNF = nqnfGeneralizeQueryF $ Left ret
   ,nqnfResOrig = fmap4 translateSyms p'
    -- Selections are incorporated in qnfSel so names don't change.
   ,nqnfResInOutNames = bimap
      (second $ qnfGeneralizeQueryF . Left)
      (second $ qnfGeneralizeQueryF . Left)
      <$> ioAssoc
  }
  -- If a column sym cannot be translated we can't recover from it.

    where
      translateSyms :: (QNFNameC Const e s,e) -> (QNFNameC Const e s,e)
      translateSyms s@(Column _ _,_) = fromMaybe err $ lookup s ioAssocN
        where
          err = error "A symbol of p can't be translated to output."
      translateSyms x = x
      ioAssocN = bimap l l <$> ioAssoc
        where
          l (e,col) = (Column col 0,e)
      (ioAssoc,ret) =
        sanitizeNameMap
          (nm
          ,updateHashSel
             qnf0
             { qnfSel = mconcat
                 $ toList
                 $ (\x -> HS.fromList x <> qnfSel qnf0)
                 . fmap2 normalizeRel
                 . toList
                 . propQnfAnd
                 . fmap3 (dropSelProdN . fst)
                 <$> p'
             })
      normalizeRel :: Rel (Expr (QNFSelName e s))
                   -> Rel (Expr (QNFSelName e s))
      normalizeRel = \case
        x@(R2 REq l r) -> if hash l < hash r
          then x
          else R2 REq r l
        x -> x
      p' :: dr (Prop (Rel (Expr (QNFNameC Const e s,e))))
      p' = qnfSubstNamesTrusting nm <$> pM
      -- The heavy lifting: translate a column to a selection.
      dropSelProdN :: QNFNameC Const e s -> QNFSelName e s
      dropSelProdN = \case
        NonSymbolName e -> Left e
        Column col _    -> Right $ dropSelProdC col
        PrimaryCol {}   -> error "Selections never have primary column names"
        where
          dropSelProdC c =
            updateHash
              QNFQuery
              { qnfColumns = qnfColumns c
               ,qnfSel = EmptyF
               ,qnfProd = EmptyF
               ,qnfOrigDEBUG' = EmptyF
               ,qnfHash = undefined
              }

qnfProdSingleton
  :: Hashables2 e s
  => Query (QNFName e s) (Either s (QNFQueryI e s))
  -> HashBag (QNFProd e s)
qnfProdSingleton = bagSingleton . HS.singleton
qnfProdSingleton2 :: Hashables2 e s =>
                    [(BQOp (QNFName e s), QNFQueryI e s, QNFQueryI e s)]
                  -> HashBag (QNFProd e s)
qnfProdSingleton2 qs =
  bagSingleton
  (HS.fromList [Q2 o (Q0 $ Right q) (Q0 $ Right q') | (o,q,q') <- qs])

-- | Trust that if e is not in the name map it's not a symbol.
getNameTrusting :: Hashables2 e s =>
                  NameMapC c e s -> e -> QNFNameC c e s
getNameTrusting nm e = either (const $ NonSymbolName e) (`Column` 0)
  $ getName nm e

-- | lookup the name assuming it's a symbol and fail if it's
-- unavailable.
getName :: Hashables2 e s =>
          NameMapC c e s -> e -> Either (QNFError e s) (QNFColC c e s)
getName nm e = maybe (Left $ error "XXX") Right $ HM.lookup e nm

qnfSubstNamesTrusting :: Hashables2 e s =>
                        NameMapC c e s
                      -> Prop (Rel (Expr e))
                      -> Prop (Rel (Expr (QNFNameC c e s,e)))
qnfSubstNamesTrusting nm = fmap3 $ \x -> (getNameTrusting nm x,x)
