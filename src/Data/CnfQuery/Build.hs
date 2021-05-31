{-# LANGUAGE CPP #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE LambdaCase                #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TupleSections             #-}
{-# LANGUAGE TypeFamilies              #-}

-- | Normalize queries for comparison

module Data.CnfQuery.Build
  (toNCNFQuery
  ,toNCNFQueryU
  ,toNCNFQueryB
  ,ncnfJoin
  ,ncnfLeftAntijoin
  ,ncnfRightAntijoin
  ,toCNF
  ,getName
  ,getNameTrusting
  ,ncnfSymbol
  ,putIdentityCNFQ
  ,putEmptyCNFQ
  ,MkQB(..)
  ,listTMaxCNF
  ,toNCNF) where

import Data.Utils.Compose
import Data.Utils.Debug
import           Control.Monad.Except
import           Data.Bifunctor
import           Data.CnfQuery.BuildProduct
import           Data.CnfQuery.BuildUtils
import           Data.CnfQuery.HashBag
import           Data.CnfQuery.SanityCheck
import           Data.CnfQuery.Types
import           Data.Functor.Identity
import qualified Data.HashMap.Lazy          as HM
import qualified Data.HashSet               as HS
import           Data.List.Extra
import qualified Data.List.NonEmpty         as NEL
import           Data.Maybe
import           Data.Query.Algebra
import           Data.Utils.AShow
import           Data.Utils.Const
import           Data.Utils.EmptyF
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.Tup
import           Data.Utils.Unsafe


-- | Turn a query into CNF and map the symbols to the CNFNames. We use
-- integers to disambiguate between symbols that refer to columns that
-- have the same data so we return all possible combinations.
toCNF :: forall e s . (HasCallStack,Hashables2 e s) =>
        (s -> Maybe [e])
      -> Query e s
      -> ListT (CNFBuild e s) (CNFQuery e s, Query (CNFName e s,e) s)
toCNF allSyms query = first snd <$> toNCNF allSyms query

toNCNF :: forall e s . (HasCallStack,Hashables2 e s) =>
        (s -> Maybe [e])
      -> Query e s
      -> ListT (CNFBuild e s) (NCNFQuery e s, Query (CNFName e s,e) s)
toNCNF allSyms query = do
  NCNFResult{..} <- toNCNFQuery =<< traverse putNCNF query
  return (putQNCNF (Identity query) ncnfResNCNF,ncnfResOrig)
  where
    putNCNF :: s -> ListT (CNFBuild e s) (NCNFQueryI e s, s)
    putNCNF s = do
      es <- maybe (throwAStr $ "Can't get symbols of " ++ ashow s) return
           $ allSyms s
      return (putQNCNF EmptyF $ ncnfSymbol es s,s)

putQNCNF :: g (Query e s)
         -> NCNFQueryDCF d c f e s
         -> NCNFQueryDCF g c f e s
putQNCNF q = fmap $ putQCNF q

putQCNF :: g (Query e s)
        -> CNFQueryDCF d c f e s
        -> CNFQueryDCF g c f e s
putQCNF q cnf = cnf{cnfOrigDEBUG'=q}


putEmptyCNFQ :: CNFQueryDCF d c f e s
             -> CNFQueryDCF EmptyF c f e s
putEmptyCNFQ cnf = cnf{cnfOrigDEBUG'=EmptyF}
putIdentityCNFQ :: Query e s
                -> CNFQueryDCF d c f e s
                -> CNFQueryDCF Identity c f e s
putIdentityCNFQ q cnf = cnf{cnfOrigDEBUG'=Identity q}


mapOp :: (op -> op') -> NCNFResultDF d f op e s -> NCNFResultDF d f op' e s
mapOp fop NCNFResult{..} = NCNFResult {
  ncnfResNCNF=ncnfResNCNF,
  ncnfResOrig=fop ncnfResOrig,
  ncnfResInOutNames=ncnfResInOutNames
  }
rebaseOp :: Hashables2 e s =>
           NCNFResultI (UQOp (CNFName e s, e)) e s
         -> Either (CNFError e s) (NCNFResultI (UQOp (CNFName e s, e)) e s)
rebaseOp res = do
  let translSym = \case
        (Column col 0,e) -> case lookup (e,col) $ ncnfResInOutNames res of
          Nothing        -> throwAStr "oops"
          Just (e',col') -> return (Column col' 0,e')
        (Column _ _,_) -> throwAStr "oops"
        (PrimaryCol {},_) -> throwAStr
          $ "Since we are refering to output we can't have"
          ++ " primary columns in op since at the very least the op itself "
          ++ " is included in the op."
        x -> return x
  op' <- traverse translSym $ ncnfResOrig res
  return
    res { ncnfResOrig = op'
        }

toNCNFQueryU :: forall e s . (HasCallStack, Hashables2 e s) =>
               UQOp e
             -> NCNFQueryI e s
             -> CNFBuild e s (NCNFResultI (UQOp (CNFName e s, e)) e s)
toNCNFQueryU o ncnfIn@(nmIn,_) = do
  -- Use input namemap
  let o' = (\x -> (getNameTrusting nmIn x,x)) <$> o
  let putO :: NCNFResultI a e s -> NCNFResultI (UQOp (CNFName e s, e)) e s
      putO = mapOp (const o')
  (sanityCheckRes ("UOP: " ++ ashow o) id =<<) $ case o of
  -- case o of
    QSel p     -> mapOp QSel <$> ncnfSelect p ncnfIn
    QProj p    -> mapOp QProj <$> ncnfProject p ncnfIn
    QGroup p e -> mapOp (uncurry QGroup) <$> ncnfAggregate p e ncnfIn
    QSort _    -> lift $ rebaseOp $ putO $ ncnfAny1 (fst <$> o') ncnfIn
    QLimit _   -> return $ putO $ ncnfAny1 (fst <$> o') ncnfIn
    QDrop _    -> return $ putO $ ncnfAny1 (fst <$> o') ncnfIn

newtype MkQB e s = MkQB { mkQB :: forall a . a -> a -> Query (CNFName e s,e) a}


-- This is meant to be used like this. But it has to be done ONCE, at
-- the END. Otherwise the order of the products may prodice different
-- interediate maximum hashes. If everything works as expected the
-- list will be the same in different order.
--
-- listTMaxCNF (ncnfResNCNF)
listTMaxCNF
  :: (Hashables2 e s,Monad m)
  => (a -> CNFQueryDCF d Either HashBag e s)
  -> ListT m a
  -> m (Maybe a)
listTMaxCNF f l = return . maximumOn (hash . f) <$> runListT l

toNCNFQueryB
  :: forall e s .
  (HasCallStack,Hashables2 e s)
  => BQOp e
  -> NCNFQueryI e s
  -> NCNFQueryI e s
  -> ListT
    (CNFBuild e s)
    (NCNFResultI (Either (BQOp (CNFName e s,e)) (MkQB e s)) e s)
toNCNFQueryB o l r =
  (sanityCheckRes ("BOP: " ++ ashow o) (Compose . either Just (const Nothing)) =<<)
  $ case o of
    QJoin p -> mapOp' QJoin <$> ncnfJoin p l r
    QProd -> do
      ret <- mapOp' (const QProd) <$> ncnfProduct l r
      when (bagNull $ cnfProd $ snd r)
        $ throwAStr
        $ "L Same in as out. R Cols: "
        ++ ashow (cnfColumns $ snd r)
      return ret
    QUnion -> lift2 $ mapOp' (const QUnion) <$> ncnfUnion l r
    -- Note: Antijoins have unambiguous outputs but the operator may
    -- have conflicting symbols. We solve the problem of symbol
    -- conflict in joins so we inherit the work from there to
    -- disambiguate the symbols in op by disambiguation but the
    -- columns are not incremented.
    QLeftAntijoin p -> do
      p' <- ncnfResOrig <$> ncnfJoin p l r
      return $ mapOp' (const $ QLeftAntijoin p') $ ncnfLeftAntijoin p' l r
    QRightAntijoin p -> do
      p' <- ncnfResOrig <$> ncnfJoin p l r
      return $ mapOp' (const $ QRightAntijoin p') $ ncnfRightAntijoin p' l r
    QProjQuery -> lift $ do
      keyAssoc <- leftAsRightKeys
      ret <- mapOp (\p -> Right $ MkQB $ \_ q -> Q1 (QProj p) (Q0 q))
        <$> ncnfProject [(kl,E0 kr) | (kl,kr) <- keyAssoc] r
      return ret
    -- All columns in the left appear on the right so just keep the
    -- columns of th right
    QDistinct -> lift $ do
      keyAssoc <- leftAsRightKeys
      mapOp (\(p,e) -> Right $ MkQB $ \_ q -> Q1 (QGroup p e) (Q0 q))
        <$> ncnfAggregate
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
    leftAsRightKeys :: CNFBuild e s [(e,e)]
    leftAsRightKeys = traverse findColOnR $ HM.toList $ nameMap l
      where
        findColOnR :: (e,CNFCol e s) -> CNFBuild e s (e,e)
        findColOnR lcol =
          maybe
            (throwAStr $ "Col in lhs not found in rhs")
            (return . (fst lcol,) . fst)
          $ find (equivCol lcol)
          $ HM.toList
          $ fst r
          where
            equivCol :: (e,CNFCol e s) -> (e,CNFCol e s) -> Bool
            equivCol (el,cl) (er,cr) =
              el == er || cnfColumns cl == cnfColumns cr
    mapOp' :: (op -> BQOp (CNFName e s,e))
           -> NCNFResultDF d f op e s
           -> NCNFResultDF d f (Either (BQOp (CNFName e s,e)) (MkQB e s)) e s
    mapOp' opf = mapOp $ Left . opf

ncnfLeftAntijoin :: Hashables2 e s =>
                   Prop (Rel (Expr (CNFName e s, e)))
                  -> NCNFQueryI e s
                  -> NCNFQueryI e s
                  -> NCNFResultI () e s
ncnfLeftAntijoin p' l r = mapOp (const ()) $ ncnfAny2 const (lso,l,r) [(rso,r,l)]
  where
    lso = QLeftAntijoin p'
    rso = QRightAntijoin p'

ncnfRightAntijoin :: Hashables2 e s =>
                    Prop (Rel (Expr (CNFName e s, e)))
                  -> NCNFQueryI e s
                  -> NCNFQueryI e s
                  -> NCNFResultI () e s
ncnfRightAntijoin p' l r =
  mapOp (const ()) $ ncnfAny2 (const id) (lso,l,r) [(rso,r,l)]
  where
    lso = QRightAntijoin p'
    rso = QLeftAntijoin p'

toNCNFQuery
  :: forall e s s0 .
  (HasCallStack,Hashables3 e s s0)
  => Query e (NCNFQueryI e s,s0)
  -> ListT (CNFBuild e s) (NCNFResultI (Query (CNFName e s,e) s0) e s)
toNCNFQuery = recur
  where
    recur = \case
      Q2 o l r -> do
        Tup2 resl resr <- recur `traverse` Tup2 l r
        mapOp
          (either
             (\o' -> Q2 o' (ncnfResOrig resl) (ncnfResOrig resr))
             (\(MkQB f) -> join $ f (ncnfResOrig resl) (ncnfResOrig resr)))
          <$> toNCNFQueryB o (ncnfResNCNF resl) (ncnfResNCNF resr)
      Q1 o ncnf -> do
        res <- recur ncnf
        lift
          $ mapOp (`Q1` ncnfResOrig res)
          <$> toNCNFQueryU o (ncnfResNCNF res)
      Q0 (s,s0) -> return
        NCNFResult
        { ncnfResNCNF = s,ncnfResOrig = Q0 s0,ncnfResInOutNames = [] }


ncnfSymbol :: forall e s. HashableCNF HashBag e s =>
             [e] -> s -> NCNFQuery e s
ncnfSymbol es s = do
    (HM.fromList $ fmap2 (putQCNF EmptyF) colAssoc, cnf)
  where
    colAssoc = [(e,cnfSymbol $ Identity $ E0 $ PrimaryCol e s 0) | e <- es]
    cnf = cnfSymbol $ toHashBag [E0 $ PrimaryCol e s 0 | e <- es]
    cnfSymbol :: HashableCNF f e s =>
                CNFProj f e s -> CNFQueryDCF Identity Either f e s
    cnfSymbol cols = updateHash CNFQuery {
      cnfColumns=Left cols,
      cnfSel=mempty,
      cnfHash=undefined,
      cnfProd=bagSingleton $ HS.singleton $ Q0 $ Left s,
      cnfOrigDEBUG'=Identity $ Q0 s}


-- | If all of namemaps are E0 return the aggr otherwise wrap in
-- projections.
ncnfCombineCols :: forall e s .
                  (Hashables2 e s, HashableCNF Identity e s) =>
                  [(e,CNFColProj e s)] -> NCNFQueryI e s
ncnfCombineCols colAssocC = (nm,cnf) where
  colGAssoc :: [(e,Expr (CNFName e s))]
  colGAssoc = [(e,runIdentity $ getConst $ cnfColumns col)
              | (e,col) <- colAssocC]
  commonCnf = cnfDropColumnsF $ snd $ headErr colAssocC
  mkCnf :: HashableCNFC c' f e s =>
          c' (CNFProj f e s) (CNFAggr f e s)
        -> CNFQueryCF c' f e s
  mkCnf a = updateHash CNFQuery {
    cnfColumns=a,
    cnfSel=cnfSel commonCnf,
    cnfProd=cnfProd commonCnf,
    cnfHash=undefined,
    cnfOrigDEBUG'=EmptyF}
  colAssoc :: [(e,CNFCol e s)] = fmap2 (mkCnf . Left . Identity) colGAssoc
  nm :: NameMap e s
  nm = HM.fromList colAssoc
  cnf :: CNFQueryI e s
  cnf = mkCnf $ Left $ toHashBag $ snd <$> colGAssoc

-- |Make a result record for a projection.
ncnfProject :: forall e e' s .
              (e' ~ (CNFName e s,e), Hashables2 e s) =>
              [(e,Expr e)]
            -> NCNFQueryI e s
            -> CNFBuild e s (NCNFResultI [(e',Expr e')] e s)
ncnfProject p l =
  cnfCached cnfProject_cache (\cc x -> cc{cnfProject_cache=x}) (p,l)
  $ ncnfProjectI p l
ncnfProjectI :: forall e e' s .
               (e' ~ (CNFName e s,e), Hashables2 e s) =>
               [(e,Expr e)]
             -> NCNFQueryI e s
             -> Either (CNFError e s) (NCNFResultI [(e',Expr e')] e s)
ncnfProjectI [] _ = return NCNFResult{
  ncnfResInOutNames=[],
  ncnfResOrig=[],
  ncnfResNCNF=(mempty,
               updateHash CNFQuery{
                  cnfColumns=Left mempty,
                  cnfProd=mempty,
                  cnfSel=mempty,
                  cnfOrigDEBUG'=EmptyF,
                  cnfHash=undefined})}
ncnfProjectI proj ncnfIn = do
  colsOutAssoc0 :: [(e, (CNFColProj e s, Expr e'))] <-
    ncnfProjCol ncnfIn `traverse2` proj
  let colsOutAssoc = colsOutAssoc0
  let ncnf = ncnfCombineCols $ fmap2 fst colsOutAssoc
  return $ mkRes colsOutAssoc ncnf
  where
    mkRes :: [(e, (CNFColProj e s,Expr e'))]
          -> NCNFQueryI e s
          -> NCNFResultI [(e', Expr e')] e s
    mkRes colsAssoc ncnfOut = NCNFResult {
      ncnfResNCNF=ncnfOut,
      ncnfResOrig=[((Column (cnfGeneralizeQueryF $ Left outCol) 0,e), expr)
                  | (e,(outCol,expr)) <- colsAssoc],
      -- There is no one-to-one correspondance of in/out names in
      -- aggregations.
      ncnfResInOutNames=[]
      }

ncnfAggregate :: forall e s e' . (e' ~ (CNFName e s,e), Hashables2 e s) =>
                [(e,Expr (Aggr (Expr e)))]
              -> [Expr e]
              -> NCNFQueryI e s
              -> CNFBuild e s
              (NCNFResultI ([(e', Expr (Aggr (Expr e')))],[Expr e']) e s)
ncnfAggregate p g l =
  cnfCached cnfAggregate_cache (\cc x -> cc{cnfAggregate_cache=x}) (p,g,l)
  $ ncnfAggregateI p g l

ncnfAggregateI :: forall e s e' . (e' ~ (CNFName e s,e), Hashables2 e s) =>
                [(e,Expr (Aggr (Expr e)))]
              -> [Expr e]
              -> NCNFQueryI e s
              -> Either (CNFError e s)
              (NCNFResultI ([(e', Expr (Aggr (Expr e')))],[Expr e']) e s)
ncnfAggregateI proj expr ncnfIn@(nmIn,_) = do
  proj' :: [(e,Expr (Aggr (CNFColProj e s,Expr (CNFName e s,e))))] <-
    traverse4 (ncnfProjCol ncnfIn) proj
  exprs :: [(CNFColProj e s,Expr (CNFName e s,e))] <-
    traverse (ncnfProjCol ncnfIn) expr
  let projCnf :: CNFQueryProj e s =
        cnfConcatProjColsUnsafe $ fmap fst $ exprs ++ toList4 proj'
  let projAggr :: [(e, Expr (CNFColAggr e s))] =
        fmap3 (cnfAggrCol (projCnf,fmap fst exprs) . fmap fst) proj'
  let aggrCnf :: CNFQueryAggr e s = cnfConcatAggrColsUnsafe $ toList3 projAggr
  let nameAssoc :: [(e, CNFColProj e s)] =
        fmap2 (aggrColToProjCol aggrCnf) projAggr
  let nmOut :: NameMap e s = HM.fromList
        $ fmap2 (cnfGeneralizeQueryF . Left) nameAssoc
  let cnfOut = cnfGeneralizeQueryF
               $ Left
               $ cnfConcatProjColsUnsafe
               $ toList2 nameAssoc
  return NCNFResult{
    ncnfResNCNF= (nmOut, cnfOut),
    ncnfResOrig= (bimap
                  (\e -> (getNameTrusting nmOut e,e))
                  (fmap3 $ \e -> (getNameTrusting nmIn e,e))
                  <$> proj,
                  fmap2 (\e -> (getNameTrusting nmIn e,e)) expr),
    -- There is no one-to-one correspondance of in/out names for
    -- aggregations
    ncnfResInOutNames =[]
  }

-- | Concatenate the columns into a single CNF assuming that they are
-- all columns of the same CNF.
cnfConcatProjColsUnsafe :: forall e s. Hashables2 e s =>
                          [CNFColProj e s] -> CNFQueryProj e s
cnfConcatProjColsUnsafe [] = updateHash CNFQuery {
  cnfColumns=Const mempty,
  cnfSel=mempty,
  cnfProd=mempty,
  cnfHash=undefined,
  cnfOrigDEBUG'=EmptyF}
cnfConcatProjColsUnsafe proj@(p:_) = updateHash CNFQuery {
  cnfColumns=Const $ toHashBag $ runIdentity . getConst . cnfColumns <$> proj,
  cnfSel=cnfSel p,
  cnfProd=cnfProd p,
  cnfHash=undefined,
  cnfOrigDEBUG'=EmptyF}
-- | Concatenate the columns into a single CNF assuming that they are
-- all columns of the same CNF.
cnfConcatAggrColsUnsafe :: forall e s. Hashables2 e s =>
                          [CNFColAggr e s] -> CNFQueryAggr e s
cnfConcatAggrColsUnsafe [] = updateHash CNFQuery {
  cnfColumns=CoConst mempty,
  cnfSel=mempty,
  cnfProd=mempty,
  cnfHash=undefined,
  cnfOrigDEBUG'=EmptyF}
cnfConcatAggrColsUnsafe proj@(p:_) = updateHash CNFQuery {
  cnfColumns=CoConst
    (toHashBag $ fmap (runIdentity . fst) $ getCoConst . cnfColumns <$> proj,
     snd $ getCoConst $ cnfColumns p),
  cnfSel=cnfSel p,
  cnfProd=cnfProd p,
  cnfHash=undefined,
  cnfOrigDEBUG'=EmptyF}

cnfAggrCol :: Hashables2 e s =>
             (CNFQueryProj e s,[CNFColProj e s])
           -> Aggr (CNFColProj e s)
           -> CNFColAggr e s
cnfAggrCol (projCols,exps) aggr = updateHash CNFQuery {
  cnfColumns=CoConst
    (Identity $ runIdentity . getConst . cnfColumns <$> aggr,
     HS.fromList $ E0 . (`Column` 0) . cnfGeneralizeQueryF . Left <$> exps),
  cnfSel=mempty,
  cnfProd=bagSingleton $
    HS.singleton $ Q0 $ Right $ cnfGeneralizeQueryF $ Left projCols,
  cnfHash=undefined,
  cnfOrigDEBUG'=EmptyF}

ncnfProjCol :: forall e s . Hashables2 e s =>
              NCNFQueryI e s
            -> Expr e
            -> Either (CNFError e s) (CNFColProj e s,Expr (CNFName e s,e))
ncnfProjCol ncnf projExp = ncnfSpecializeQueryF ncnf >>= \case
  Left (projNm, projCnf) -> let
    cnf = cnfMapColumns
          (const $ Const $ Identity
            -- projNm points to projCnf
            $ getNameTrusting projNm <$> projExp >>= expandName)
          projCnf
    in return (cnf,exprPair)
  Right (aggrNm,aggrCnf) -> return (
    updateHash CNFQuery {
        cnfColumns=
            Const $ Identity $ generalizeName . getNameTrusting aggrNm <$> projExp,
        cnfSel=mempty,
        cnfHash=undefined,
        cnfProd=bagSingleton
          $ HS.singleton
          $ Q0 $ Right
          $ cnfGeneralizeQueryF $ Right aggrCnf,
        cnfOrigDEBUG'=EmptyF},exprPair)
  where
    exprPair :: Expr (CNFName e s, e)
    exprPair = go <$> projExp where
      go :: e -> (CNFName e s, e)
      go e = (getNameTrusting (fst ncnf) e,e)
    generalizeName :: CNFNameC CoConst e s -> CNFName e s
    generalizeName = cnfMapName $ Right . getCoConst
    expandName :: CNFNameC Const e s -> Expr (CNFName e s)
    expandName = \case
      Column CNFQuery{cnfColumns=Const (Identity expr)} _ -> expr
      PrimaryCol e s i -> E0 $ PrimaryCol e s i
      NonSymbolName e -> E0 $ NonSymbolName e


ncnfAny1 :: forall e s . Hashables2 e s =>
           UQOp (CNFName e s)
         -> NCNFQueryI e s
         -> NCNFResultI () e s
ncnfAny1 o (nm, cnf) = NCNFResult {
  ncnfResNCNF=(nmRet,cnfRet),
  ncnfResOrig=(),
  ncnfResInOutNames=[((e,inC),(e,outC)) | ((e,inC),outC) <- triplets]
  }
  where
    cnfRet :: CNFQueryI e s
    cnfRet = wrapOuterCnf
      $ Left $ toHashBag [E0  $ Column c 0 | ((_,c),_) <- triplets]
    nmRet :: NameMap e s
    nmRet = HM.fromList [(e,outC) | ((e,_),outC) <- triplets]
    wrapOuterCnf :: HashableCNFC c f e s =>
                   c (CNFProj f e s) (CNFAggr f e s)
                 -> CNFQueryDCF EmptyF c f e s
    wrapOuterCnf p = updateHash CNFQuery {
      cnfColumns=p,
      cnfSel=mempty,
      cnfHash=undefined,
      cnfProd=cnfProdSingleton $ Q1 o $ Q0 $ Right cnf,
      cnfOrigDEBUG'=EmptyF}
    triplets :: [((e, CNFCol e s), CNFCol e s)]
    triplets = [((e,inC),wrapOuterCnf $ Left $ Identity $ E0 $ Column inC 0)
               | (e,inC) <- HM.toList nm]


-- | Given a cnf that contains a col `in` cnfProduct(cnf) get the top
-- level column. eg
--
--   toOutCol CNF[_|_ , S, [{Q0 <C1,C2,C3}]] C2
--   ==> CNF[C2 , S, [{Q0 <C1,C2,C3>}]]
toOutColumnUnsafe :: HashableCNF f e s =>
                    CNFQueryF f e s -> CNFCol e s -> CNFCol e s
toOutColumnUnsafe outCnf col =
  runIdentity
  $ cnfTravColumns (const $ Identity $ Left $ Identity $ E0 $ Column col 0)
  outCnf

-- Make an NCNF with alternative represnetations. Select just cooses
-- either nml or nmr.
ncnfAny2 :: forall e s . Hashables2 e s =>
           (forall a . a -> a -> a)
         -> (BQOp (CNFName e s,e), NCNFQueryI e s, NCNFQueryI e s)
         -> [(BQOp (CNFName e s,e), NCNFQueryI e s, NCNFQueryI e s)]
         -> NCNFResultI (BQOp (CNFName e s,e)) e s
ncnfAny2 select q0@(op,(nml, _),(nmr, _)) qs = NCNFResult {
  ncnfResNCNF=(nm, outCnf),
  ncnfResOrig=op,
  ncnfResInOutNames=[(c,toOutColumnUnsafe outCnf <$> c)
                    | c <- HM.toList $ select nml nmr]
  }
  where
    outCnf :: CNFQueryI e s
    outCnf = wrapOuterCnf $ Left $ toHashBag
             [ E0 $ Column c 0 | c <- HM.elems $ select nml nmr]
    nm :: NameMap e s
    nm = toOutColumnUnsafe outCnf <$> select nml nmr
    wrapOuterCnf p = updateHash CNFQuery {
      cnfColumns=p,
      cnfSel=mempty,
      cnfHash=undefined,
      cnfProd=cnfProdSingleton2
        [(fst <$> o,q,q')
        | (o,(_,q),(_,q')) <- q0:qs],
      cnfOrigDEBUG'=EmptyF}

-- |The columns of the cnf are the columns of the first argument.
ncnfUnion :: forall e s . (Hashables2 e s) =>
            NCNFQueryI e s
          -> NCNFQueryI e s
          -> Either (CNFError e s) (NCNFResultI () e s)
ncnfUnion = go
  where
    go :: NCNFQueryI e s
       -> NCNFQueryI e s
       -> Either (CNFError e s) (NCNFResultI () e s)
    go (nm, cnf) (nm', cnf') = do
      -- XXX: Assert that keys are actually a subset.
      when (HM.keys nm' `subset` HM.keys nm)
        $ throwError $ CNFSchemaMismatch (HM.keys nm) (HM.keys nm')
      let outCols :: [(e,CNFCol e s)] =
            [(e,toOutColumnUnsafe (cnfRet (\ _ _ -> EmptyF) EmptyF) c)
            | (e,c) <- HM.toList nm]
      return NCNFResult {
        ncnfResNCNF=(HM.fromList outCols,
                     cnfRet bagMap $ toHashBag $ snd <$> outCols),
        ncnfResOrig=(),
        ncnfResInOutNames=do
            -- nm is a superset of nm' so c will always be in nm
            (e,cPreOut NEL.:| cs) <-
              HM.toList $ HM.unionWith (<>) (return <$> nm) (return <$> nm')
            cIn <- cPreOut:cs
            return ((e,cIn),(e,cnfRet fmap $ Identity cPreOut))
        }
      where
        subset xs = all (`elem` xs)
        cnfRet :: HashableCNF f' e s =>
                 ((CNFColC c e1 s1 -> Expr (CNFNameC c e1 s1)) ->
                  f (CNFCol e s) -> CNFProj f' e s)
               -> f (CNFCol e s) -> CNFQueryF f' e s
        cnfRet fmap' colIn = updateHash $ cnf{
          cnfColumns=Left $ fmap' (E0 . (`Column` 0)) colIn,
          cnfSel=mempty,
          cnfProd=cnfProdSingleton $ Q2 QUnion (Q0 $ Right cnf) (Q0 $ Right cnf')}

ncnfJoin :: Hashables2 e s =>
           Prop (Rel (Expr e))
         -> NCNFQueryI e s
         -> NCNFQueryI e s
         -> ListT (CNFBuild e s)
         (NCNFResultI (Prop (Rel (Expr (CNFNameC Either e s, e)))) e s)
ncnfJoin p l r = do
  resProd <- ncnfProduct l r
  resSel <- lift $ ncnfSelect p $ ncnfResNCNF resProd
  case ncnfResInOutNames resProd `chainAssoc` ncnfResInOutNames resSel of
    Nothing    -> throwAStr "Prod/sel symbol translation in/out maps don't match"
    Just finIO -> return resSel{ncnfResInOutNames=finIO}

ncnfSelect :: forall e s . Hashables2 e s =>
             Prop (Rel (Expr e))
           -> NCNFQueryI e s
           -> CNFBuild e s
           (NCNFResultI (Prop (Rel (Expr (CNFName e s, e)))) e s)
ncnfSelect p l =
  cnfCached cnfSelect_cache (\cc x -> cc{cnfSelect_cache=x}) (p,l)
  $ ncnfSelectI p l

ncnfSelectI :: forall e s . Hashables2 e s =>
             Prop (Rel (Expr e))
           -> NCNFQueryI e s
           -> Either (CNFError e s)
           (NCNFResultI (Prop (Rel (Expr (CNFName e s, e)))) e s)
ncnfSelectI p q = ncnfSpecializeQueryF q <&> \case
  Left proj -> putName $ selInternal proj
  Right aggr -> putName $ selInternal $ aggrToProjI aggr
  where
    aggrToProjI = runIdentity . ncnfLiftC1 (Identity . ncnfAggrAsProj)
    selInternal = ncnfSelectInternal (Identity p)
    putName = mapOp $ fmap3 (first $ cnfMapName $ Left . getConst) . runIdentity

-- Selection refers to the output columns (obviously since we have no
-- other way of accessing the input columns). This means that the
-- correct implementation is cyclical. Therefore we drop the selection
-- and product info aspect from all symbols in cnfSel. It is obvious
-- that they refer to the product and selection if the query itself.
--
-- Also selection is under the proj/aggr operation. Therefore if we
-- are inserting into an aggregated query we need to first wrap it
-- into a projection.
--
-- Nothing in prop means drop selection from query
ncnfSelectInternal :: forall dr f e s . (Functor dr,HashableCNF f e s,Foldable dr) =>
                     dr (Prop (Rel (Expr e)))
                   -> NCNFQueryCF Const f e s
                   -> NCNFResultF f (dr (Prop (Rel (Expr (CNFNameC Const e s, e))))) e s
ncnfSelectInternal pM (nm,cnf0) =
  NCNFResult
  { ncnfResNCNF = ncnfGeneralizeQueryF $ Left ret
   ,ncnfResOrig = fmap4 translateSyms p'
    -- Selections are incorporated in cnfSel so names don't change.
   ,ncnfResInOutNames = bimap
      (second $ cnfGeneralizeQueryF . Left)
      (second $ cnfGeneralizeQueryF . Left)
      <$> ioAssoc
  }
  -- If a column sym cannot be translated we can't recover from it.

    where
      translateSyms :: (CNFNameC Const e s,e) -> (CNFNameC Const e s,e)
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
             cnf0
             { cnfSel = mconcat
                 $ toList
                 $ (\x -> HS.fromList x <> cnfSel cnf0)
                 . fmap2 normalizeRel
                 . toList
                 . propCnfAnd
                 . fmap3 (dropSelProdN . fst)
                 <$> p'
             })
      normalizeRel :: Rel (Expr (CNFSelName e s))
                   -> Rel (Expr (CNFSelName e s))
      normalizeRel = \case
        x@(R2 REq l r) -> if hash l < hash r
          then x
          else R2 REq r l
        x -> x
      p' :: dr (Prop (Rel (Expr (CNFNameC Const e s,e))))
      p' = cnfSubstNamesTrusting nm <$> pM
      dropSelProdN :: CNFNameC Const e s -> CNFSelName e s
      dropSelProdN = \case
        NonSymbolName e -> Left e
        Column col _ -> Right $ dropSelProdC col
        PrimaryCol {} -> error "Selections never have primary column names"
        where
          dropSelProdC c =
            updateHash
              CNFQuery
              { cnfColumns = cnfColumns c
               ,cnfSel = EmptyF
               ,cnfProd = EmptyF
               ,cnfOrigDEBUG' = EmptyF
               ,cnfHash = undefined
              }

cnfProdSingleton :: Hashables2 e s =>
                   Query (CNFName e s) (Either s (CNFQueryI e s))
                 -> HashBag (CNFProd e s)
cnfProdSingleton = bagSingleton . HS.singleton
cnfProdSingleton2 :: Hashables2 e s =>
                    [(BQOp (CNFName e s), CNFQueryI e s, CNFQueryI e s)]
                  -> HashBag (CNFProd e s)
cnfProdSingleton2 qs =
  bagSingleton
  (HS.fromList [Q2 o (Q0 $ Right q) (Q0 $ Right q') | (o,q,q') <- qs])

-- | Trust that if e is not in the name map it's not a symbol.
getNameTrusting :: Hashables2 e s =>
                  NameMapC c e s -> e -> CNFNameC c e s
getNameTrusting nm e = either (const $ NonSymbolName e) (`Column` 0)
  $ getName nm e

-- | lookup the name assuming it's a symbol and fail if it's
-- unavailable.
getName :: Hashables2 e s =>
          NameMapC c e s -> e -> Either (CNFError e s) (CNFColC c e s)
getName nm e = maybe (Left $ error "XXX") Right $ HM.lookup e nm

cnfSubstNamesTrusting :: Hashables2 e s =>
                        NameMapC c e s
                      -> Prop (Rel (Expr e))
                      -> Prop (Rel (Expr (CNFNameC c e s,e)))
cnfSubstNamesTrusting nm = fmap3 $ \x -> (getNameTrusting nm x,x)
