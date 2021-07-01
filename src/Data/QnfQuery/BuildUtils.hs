{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# OPTIONS_GHC -Wno-unused-local-binds -Wno-simplifiable-class-constraints #-}
module Data.QnfQuery.BuildUtils
  ( qnfAsProd
  , qnfMapColumns
  , qnfMapName
  , nqnfAggrAsProj
  , aggrColToProjCol
  , nqnfAsProj
  , nqnfLiftC1
  , nqnfLiftC2
  , qnfIsColOf
  , qnfDropColumnsC
  , qnfDropColumnsF
  , qnfTravColumns
  , qnfSpecializeQueryF
  , qnfGeneralizeQueryF
  , nqnfGeneralizeQueryF
  , nqnfSpecializeQueryF
  , nqnfToQnf
  , nqnfModQueryDebug
  , qnfGetCols
  , qnfOrigDEBUG
  , sanitizeNameMap
  , chainAssoc
  ) where

import Data.Utils.Tup
import Data.Utils.Function
import Data.Utils.Functors
import Data.Utils.Unsafe
import Data.Utils.Const
import Data.Utils.Hashable
import           Data.Bifunctor
import           Data.Bitraversable
import           Data.Functor.Identity
import qualified Data.HashMap.Lazy                as HM
import qualified Data.HashSet                     as HS
import           Data.Query.Algebra
import           Data.Utils.AShow
import           Data.QnfQuery.HashBag
import           Data.QnfQuery.Types
import           Data.Utils.EmptyF

qnfOrigDEBUG :: QNFQuery e s -> Query e s
qnfOrigDEBUG = runIdentity . qnfOrigDEBUG'


nqnfAsProj :: forall f e s . (Applicative f, Foldable f, HashableQNF f e s) =>
             NQNFQueryF f e s -> Either (QNFError e s) (NQNFQueryProjF f e s)
nqnfAsProj (nm,qnf) = case qnfColumns qnf of
  Left projCols  ->
    (, QNFQuery {qnfColumns=Const projCols,
                 qnfSel=qnfSel qnf,
                 qnfProd=qnfProd qnf,
                 qnfHash=qnfHash qnf, -- remember: hash (Cost x) == hash (Left x)
                 qnfOrigDEBUG'=qnfOrigDEBUG' qnf})
    <$> HM.traverseWithKey (const specializeColProj) nm
  Right aggrCols -> nqnfAggrAsProj
     . (,QNFQuery {qnfColumns=CoConst aggrCols,
                   qnfSel=qnfSel qnf,
                   qnfProd=qnfProd qnf,
                   qnfHash=qnfHash qnf,  -- remember: hash (Cost x) == hash (Left x)
                   qnfOrigDEBUG'=qnfOrigDEBUG' qnf})
     <$> HM.traverseWithKey (const specializeColAggr) nm
  where
    -- We either need only aggr or only projections, not both.
    err :: Either (QNFError e s) (QNFColC c e s)
    err = throwAStr "Specialization found unexpected token."
    specializeColProj :: QNFCol e s -> Either (QNFError e s) (QNFColProj e s)
    specializeColProj = either return (const err) . qnfSpecializeQueryF
    specializeColAggr :: QNFCol e s -> Either (QNFError e s) (QNFColAggr e s)
    specializeColAggr = either (const err) return . qnfSpecializeQueryF

nqnfLiftC2 :: forall e s m c1 c2 c3 x .
             (Bifunctor c1,Bifunctor c2,Bifunctor c3, Functor m,
              Hashables2 e s,
              HashableQNFC c1 [] e s,HashableQNFC c1 HashBag e s,
              HashableQNFC c2 [] e s,HashableQNFC c2 HashBag e s,
              HashableQNFC c3 [] e s,HashableQNFC c3 HashBag e s) =>
            (NQNFQueryCF c1 [] e s ->
             NQNFQueryCF c2 [] e s ->
             m (x,NQNFQueryCF c3 [] e s))
          -> NQNFQueryCF c1 HashBag e s
          -> NQNFQueryCF c2 HashBag e s
          -> m (x,NQNFQueryCF c3 HashBag e s)
nqnfLiftC2 f l r = fmap3 dropLi $ f (putLi <$> l) (putLi <$> r) where
  dropLi :: forall c . (HashableQNFC c [] e s, HashableQNFC c HashBag e s, Bifunctor c) =>
           QNFQueryCF c [] e s -> QNFQueryCF c HashBag e s
  dropLi = qnfMapColumns $ bimap toHashBag $ first toHashBag
  putLi :: forall c . (HashableQNFC c [] e s, HashableQNFC c HashBag e s,Bifunctor c) =>
          QNFQueryCF c HashBag e s -> QNFQueryCF c [] e s
  putLi = qnfMapColumns $ bimap toList $ first toList

nqnfLiftC1 :: forall e s m c1 c2 .
             (Bifunctor c1, Bifunctor c2, Functor m, Hashables2 e s,
              HashableQNFC c1 [] e s,HashableQNFC c1 HashBag e s,
              HashableQNFC c2 [] e s,HashableQNFC c2 HashBag e s) =>
             (NQNFQueryCF c1 [] e s -> m (NQNFQueryCF c2 [] e s))
           -> NQNFQueryCF c1 HashBag e s
           -> m (NQNFQueryCF c2 HashBag e s)
nqnfLiftC1 f nqnf = fmap2 dropLi $ f $ putLi <$> nqnf where
  dropLi :: QNFQueryCF c2 [] e s -> QNFQueryCF c2 HashBag e s
  dropLi = qnfMapColumns $ bimap toHashBag $ first toHashBag
  putLi :: QNFQueryCF c1 HashBag e s -> QNFQueryCF c1 [] e s
  putLi = qnfMapColumns $ bimap toList $ first toList


qnfAsProd :: (HashableQNF f' e s, HashableQNF f e s, Foldable f) =>
            QNFQueryF f e s -> QNFQueryCF EmptyF2 f' e s
qnfAsProd qnf = updateHash QNFQuery {
  qnfColumns=EmptyF2,
  qnfSel=mempty,
  qnfProd=bagSingleton $ HS.singleton $ Q0 $ Right $ runIdentity
    $ qnfTravColumns (Identity . bimap toHashBag (first toHashBag))
    qnf,
  qnfHash=undefined,
  qnfOrigDEBUG'=qnfOrigDEBUG' qnf
  }

qnfMapColumns :: (HashableQNFSPCF  sel_f prod_f col_f' f e s,
                 HashableQNFSPCF  sel_f prod_f col_f' f' e s) =>
                (col_f (QNFProj f e s) (QNFAggr f e s) ->
                  col_f' (QNFProj f' e s) (QNFAggr f' e s))
               -> QNFQuerySPDCF sel_f prod_f EmptyF col_f f e s
               -> QNFQuerySPDCF sel_f prod_f EmptyF col_f' f' e s
qnfMapColumns f = updateHashCol . qnfMapColumns' f
-- | Map but retain the hash. UNSAFE
qnfMapColumns' :: (HashableQNFSPCF  sel_f prod_f col_f' f e s,
                 HashableQNFSPCF  sel_f prod_f col_f' f' e s) =>
                 (col_f (QNFProj f e s) (QNFAggr f e s) ->
                  col_f' (QNFProj f' e s) (QNFAggr f' e s))
               -> QNFQuerySPDCF sel_f prod_f EmptyF col_f f e s
               -> QNFQuerySPDCF sel_f prod_f EmptyF col_f' f' e s
qnfMapColumns' f QNFQuery{..} = QNFQuery{
  qnfColumns=f qnfColumns,
  qnfSel=qnfSel,
  qnfProd=qnfProd,
  qnfHash=qnfHash,
  qnfOrigDEBUG'=EmptyF}
{-# SCC qnfMapColumns #-}
{-# INLINE qnfMapColumns #-}

qnfMapName :: forall c c' e s . HashableQNFC c' Identity e s =>
             (c (QNFProj Identity e s) (QNFAggr Identity e s) ->
              c' (QNFProj Identity e s) (QNFAggr Identity e s))
           -> QNFNameC c e s
           -> QNFNameC c' e s
qnfMapName f = \case
  Column c i -> Column (qnfMapColumns f c) i
  NonSymbolName e -> NonSymbolName e
  PrimaryCol e s i -> PrimaryCol e s i

aggrColToProjCol :: Hashables2 e s =>
                   QNFQueryAggr e s -> Expr (QNFColAggr e s) -> QNFColProj e s
aggrColToProjCol qnfAggr expr = updateHash QNFQuery {
  qnfColumns=
      Const $ Identity $ (`Column` 0) . qnfGeneralizeQueryF . Right <$> expr,
  qnfSel=mempty,
  qnfProd=
      bagSingleton $ HS.singleton
      $ Q0 $ Right $ qnfGeneralizeQueryF $ Right qnfAggr,
  qnfHash=undefined,
  qnfOrigDEBUG'=EmptyF
  }

-- | Wrap the aggregation QNF with a projection QNF.
nqnfAggrAsProj :: forall f e s .
                (Applicative f, Foldable f, HashableQNF f e s) =>
                NQNFQueryAggrF f e s -> NQNFQueryProjF f e s
nqnfAggrAsProj (nm,qnfAggr) = (nmProj,qnfProj)
  where
    qnfProj :: QNFQueryDCF EmptyF Const f e s
    qnfProj = updateHash QNFQuery {
      qnfColumns=Const cols,
      qnfSel=mempty,
      qnfProd=bagSingleton $ HS.singleton $ Q0 $ Right prod,
      qnfHash=(fst3 $ qnfHash qnfAggr,undefined,undefined),
      qnfOrigDEBUG'=qnfOrigDEBUG' qnfAggr
      }
    prod :: (QNFQueryI e s)
    prod = qnfMapColumns (bimap toHashBag $ first toHashBag)
           $ qnfGeneralizeQueryF $ Right qnfAggr
    nmProj :: NameMapProj e s
    nmProj = HM.map toProjCol nm where
      toProjCol :: QNFColC CoConst e s -> QNFColC Const e s
      toProjCol = qnfColAggrToProj
                   $ ((Identity . E0 . (`Column` 0) . fromJustErr)
                       ... flip lookup)
                   $ extractAggr
                   <$> toList cols
    extractAggr :: Expr (QNFName e2 s2)
                -> (Aggr (Expr (QNFName e2 s2)), QNFCol e2 s2)
    extractAggr = \case
      WrappedAggrColumn qnf ag -> (ag, qnf);
      _ -> error "qnfAggrAsProjCol ret val does not match WrappedAggrColumn pattern"
    qnfColAggrToProj :: (Aggr (Expr (QNFName e s)) -> QNFProj Identity e s)
                      -> QNFColAggr e s -> QNFColProj e s
    qnfColAggrToProj f = qnfMapColumns $ Const . f . runIdentity . fst . getCoConst
    cols :: QNFProj f e s
    cols = qnfAggrAsProjCol qnfAggr

-- |The expected pattern of all the proj wrapped columns.
pattern WrappedAggrColumn :: forall e s .
                            QNFCol e s
                          -> Aggr (Expr (QNFName e s))
                          -> Expr (QNFName e s)
pattern WrappedAggrColumn qnf aggr <-
  E0 (Column qnf@QNFQuery{qnfColumns=Right (Identity aggr,_)} 0)
qnfAggrAsProjCol :: forall f e s . (HashableQNF f e s, Applicative f) =>
                   QNFQueryAggrF f e s -> QNFProj f e s
qnfAggrAsProjCol qnf = colsProj
  where
    aggrColAsName :: QNFQueryAggrF Identity e s -> QNFName e s
    aggrColAsName = (`Column` 0) . qnfGeneralizeQueryF . Right
    colsProj :: f (Expr (QNFName e s))
    colsProj = E0 . aggrColAsName <$> qnfGetCols qnf

qnfGetCols :: (HashableQNFC c Identity e s,
              HashableQNFC c f e s,
              Bitraversable c, Applicative f) =>
             QNFQueryDCF EmptyF c f e s
           -> f (QNFQueryDCF EmptyF c Identity e s)
qnfGetCols = qnfTravColumns $ bitraverse toList' $ bitraverse toList' pure where
  toList' = fmap Identity

qnfIsColOf :: forall sel_f prod_f f e s .
             (HashableQNFSPCF sel_f prod_f Either Identity e s,
              HashableQNFSPCF sel_f prod_f Either f e s,
              Foldable f,
              Hashables2 e s) =>
             QNFQuerySPDCF sel_f prod_f EmptyF Either Identity e s
           -> QNFQuerySPDCF sel_f prod_f EmptyF Either f e s
           -> Bool
qnfIsColOf col qnf = uncolCol col == uncolQ qnf
                     && case (qnfColumns col, qnfColumns qnf) of
                         (Right (Identity x,_), Right (xs,_)) -> x `elem` xs
                         (Left (Identity x), Left xs) -> x `elem` xs
                         _ -> error "dropColumns equaliy makes this unreachable"
  where
    uncolCol :: QNFQuerySPDCF sel_f prod_f EmptyF Either Identity e s
             -> QNFQuerySPDCF sel_f prod_f EmptyF EmptyF2 EmptyF e s
    uncolCol = qnfDropColumnsC
    uncolQ :: QNFQuerySPDCF sel_f prod_f EmptyF Either f e s
           -> QNFQuerySPDCF sel_f prod_f EmptyF EmptyF2 EmptyF e s
    uncolQ = qnfDropColumnsC

qnfDropColumnsF :: forall c f e s .
                  (HashableQNFC c f e s,
                   HashableQNFC c EmptyF e s,
                   Bitraversable c) =>
                  QNFQueryCF c f e s -> QNFQueryCF c EmptyF e s
qnfDropColumnsF = qnfMapColumns $ bimap f (first f) where
  f = const EmptyF
qnfDropColumnsC :: (HashableQNFC col_f f' e s,
                   HashableQNFSPCF sel_f prod_f col_f f e s,
                   Bitraversable col_f) =>
                  QNFQuerySPDCF sel_f prod_f EmptyF col_f f e s
                -> QNFQuerySPDCF sel_f prod_f EmptyF EmptyF2 f' e s
qnfDropColumnsC = qnfMapColumns $ const EmptyF2

qnfTravColumns :: forall g sel_f prod_f col_f col_f' f f' e s .
                 (HashableQNFSPCF sel_f prod_f col_f' f e s,
                  HashableQNFSPCF sel_f prod_f col_f' f' e s,
                  Functor g) =>
                 (col_f (QNFProj f e s) (QNFAggr f e s) ->
                  g (col_f' (QNFProj f' e s) (QNFAggr f' e s)))
               -> QNFQuerySPDCF sel_f prod_f EmptyF col_f f e s
               -> g (QNFQuerySPDCF sel_f prod_f EmptyF col_f' f' e s)
qnfTravColumns f qnf = updateHashCol <$> qnfTravColumns' f qnf

qnfTravColumns' :: forall g sel_f prod_f col_f col_f' f f' e s .
                 (HashableQNFSPCF sel_f prod_f col_f' f e s,
                  HashableQNFSPCF sel_f prod_f col_f' f' e s,
                  Functor g) =>
                 (col_f (QNFProj f e s) (QNFAggr f e s) ->
                  g (col_f' (QNFProj f' e s) (QNFAggr f' e s)))
               -> QNFQuerySPDCF sel_f prod_f EmptyF col_f f e s
               -> g (QNFQuerySPDCF sel_f prod_f EmptyF col_f' f' e s)
qnfTravColumns' f qnf = onCols <$> f (qnfColumns qnf) where
  onCols :: col_f' (QNFProj f' e s) (QNFAggr f' e s)
         -> QNFQuerySPDCF sel_f prod_f EmptyF col_f' f' e s
  onCols cols = QNFQuery{
    qnfColumns=cols,
    qnfSel=qnfSel qnf,
    qnfProd=qnfProd qnf,
    qnfHash=qnfHash qnf,
    qnfOrigDEBUG'=qnfOrigDEBUG' qnf}
-- | Unsafe map. For maps that don't change the hash.
nqnfMapColumns' :: forall col_f col_f' f f' e s .
                  (HashableQNFSPCF HS.HashSet HashBag col_f' f e s,
                   HashableQNFSPCF HS.HashSet HashBag col_f' f' e s,
                   HashableQNFSPCF HS.HashSet HashBag col_f' Identity e s) =>
                  (col_f (QNFProj Identity e s) (QNFAggr Identity e s) ->
                   col_f' (QNFProj Identity e s) (QNFAggr Identity e s))
                -> (col_f (QNFProj f e s) (QNFAggr f e s) ->
                    col_f' (QNFProj f' e s) (QNFAggr f' e s))
                -> NQNFQueryDCF EmptyF col_f f e s
                -> NQNFQueryDCF EmptyF col_f' f' e s
nqnfMapColumns' fcol f (nm,qnf) = (qnfMapColumns' fcol <$> nm,qnfMapColumns' f qnf)

nqnfGeneralizeQueryF :: HashableQNF f e s =>
                       Either (NQNFQueryProjF f e s) (NQNFQueryAggrF f e s)
                     -> NQNFQueryF f e s
nqnfGeneralizeQueryF = either
  (nqnfMapColumns' (Left . getConst) $ Left . getConst)
  (nqnfMapColumns' (Right . getCoConst) $ Right . getCoConst)

qnfGeneralizeQueryF :: HashableQNF f e s =>
                      Either (QNFQueryProjF f e s) (QNFQueryAggrF f e s)
                    -> QNFQueryF f e s
qnfGeneralizeQueryF = either
  (qnfMapColumns' $ Left . getConst)
  (qnfMapColumns' $ Right . getCoConst)
qnfSpecializeQueryF :: forall f e s . HashableQNF f e s =>
                      QNFQueryF f e s
                    -> Either (QNFQueryProjF f e s) (QNFQueryAggrF f e s)
qnfSpecializeQueryF qnf = case qnfColumns qnf of
  Left x  -> Left $ mkQ $ Const x
  Right x -> Right $ mkQ $ CoConst x
  where
    mkQ :: forall c . HashableQNFC c f e s =>
          c (QNFProj f e s) (QNFAggr f e s)
        -> QNFQueryDCF EmptyF c f e s
    mkQ c = QNFQuery{
      qnfColumns=c,
      qnfSel=qnfSel qnf,
      qnfProd=qnfProd qnf,
      qnfHash=qnfHash qnf,
      qnfOrigDEBUG'=EmptyF}
nqnfSpecializeQueryF :: forall f e s . HashableQNF f e s =>
                       NQNFQueryF f e s
                     -> Either (QNFError e s )
                     (Either (NQNFQueryProjF f e s) (NQNFQueryAggrF f e s))
nqnfSpecializeQueryF (nm,qnf) = case qnfSpecializeQueryF qnf of
  Left projQnf -> Left . (,projQnf) <$> HM.traverseWithKey (const go) nm where
    go :: QNFCol e s
       -> Either (QNFError e s) (QNFColProj e s)
    go = qnfTravColumns' $ \case
      Left a -> return $ Const a
      Right _ -> Left $ QNFErrorMsg "Mismatch: QNF is proj, namemap refers to aggr"
  Right aggrQnf -> Right . (,aggrQnf) <$> HM.traverseWithKey (const go) nm where
    go = qnfTravColumns' $ \case
      Right a -> return $ CoConst a
      Left _ -> Left $ QNFErrorMsg "Mismatch: QNF is aggr, namemap refers to proj"
nqnfToQnf :: NQNFQuery e s -> QNFQuery e s
nqnfToQnf = snd
nqnfModQueryDebug :: (d (Query e s) -> d' (Query e s))
                  -> NQNFQueryDCF d c f e s
                  -> NQNFQueryDCF d' c f e s
nqnfModQueryDebug f (nm,qnf) = (nm,qnf{qnfOrigDEBUG'=f $ qnfOrigDEBUG' qnf})

-- |Assuming the qnf has been updated internally, update the namemap
-- too.
sanitizeNameMap :: forall d c f e s .
                  (Hashables2 e s, HashableQNFC c Identity e s) =>
                  NQNFQueryDCF d c f e s
                -> ([((e,QNFColC c e s),(e,QNFColC c e s))],
                   NQNFQueryDCF d c f e s)
sanitizeNameMap (nm,qnf) = -- assertSane
  (toList nmPairs, (snd . snd <$> nmPairs,qnf))
  where
    nmPairs :: HM.HashMap e ((e, QNFColC c e s),(e, QNFColC c e s))
    nmPairs = HM.mapWithKey promoteCol nm where
      promoteCol :: a
                 -> QNFQueryDCF EmptyF c Identity e s
                 -> ((a,QNFQueryDCF EmptyF c Identity e s),
                    (a,QNFQueryDCF EmptyF c Identity e s))
      promoteCol e col = -- XXX: do we really need to update all hashes?
        ((e,col),(e,col{
                     qnfProd=qnfProd qnf,
                     qnfSel=qnfSel qnf,
                     qnfHash=(fst3 $ qnfHash col,
                              snd3 $ qnfHash qnf,
                              trd3 $ qnfHash qnf)}))

    assertSane x =
      if all (\c -> qnfSel qnf == qnfSel c && qnfProd c == qnfProd qnf) outColumns
      then x
      else error "bad sanitization of namemaps"
    outColumns = snd . snd <$> toList nmPairs

chainAssoc :: Eq b => [(a,b)] -> [(b,c)] -> Maybe [(a,c)]
chainAssoc ab bc = sequenceA [(a,) <$> lookup b bc | (a,b) <- ab]
