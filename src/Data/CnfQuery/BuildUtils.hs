{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# OPTIONS_GHC -Wno-unused-local-binds -Wno-simplifiable-class-constraints #-}
module Data.CnfQuery.BuildUtils
  ( cnfAsProd
  , cnfMapColumns
  , cnfMapName
  , ncnfAggrAsProj
  , aggrColToProjCol
  , ncnfAsProj
  , ncnfLiftC1
  , ncnfLiftC2
  , cnfIsColOf
  , cnfDropColumnsC
  , cnfDropColumnsF
  , cnfTravColumns
  , cnfSpecializeQueryF
  , cnfGeneralizeQueryF
  , ncnfGeneralizeQueryF
  , ncnfSpecializeQueryF
  , ncnfToCnf
  , ncnfModQueryDebug
  , cnfGetCols
  , cnfOrigDEBUG
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
import           Data.CnfQuery.HashBag
import           Data.CnfQuery.Types
import           Data.Utils.EmptyF

cnfOrigDEBUG :: CNFQuery e s -> Query e s
cnfOrigDEBUG = runIdentity . cnfOrigDEBUG'


ncnfAsProj :: forall f e s . (Applicative f, Foldable f, HashableCNF f e s) =>
             NCNFQueryF f e s -> Either (CNFError e s) (NCNFQueryProjF f e s)
ncnfAsProj (nm,cnf) = case cnfColumns cnf of
  Left projCols  ->
    (, CNFQuery {cnfColumns=Const projCols,
                 cnfSel=cnfSel cnf,
                 cnfProd=cnfProd cnf,
                 cnfHash=cnfHash cnf, -- remember: hash (Cost x) == hash (Left x)
                 cnfOrigDEBUG'=cnfOrigDEBUG' cnf})
    <$> HM.traverseWithKey (const specializeColProj) nm
  Right aggrCols -> ncnfAggrAsProj
     . (,CNFQuery {cnfColumns=CoConst aggrCols,
                   cnfSel=cnfSel cnf,
                   cnfProd=cnfProd cnf,
                   cnfHash=cnfHash cnf,  -- remember: hash (Cost x) == hash (Left x)
                   cnfOrigDEBUG'=cnfOrigDEBUG' cnf})
     <$> HM.traverseWithKey (const specializeColAggr) nm
  where
    -- We either need only aggr or only projections, not both.
    err :: Either (CNFError e s) (CNFColC c e s)
    err = throwAStr "Specialization found unexpected token."
    specializeColProj :: CNFCol e s -> Either (CNFError e s) (CNFColProj e s)
    specializeColProj = either return (const err) . cnfSpecializeQueryF
    specializeColAggr :: CNFCol e s -> Either (CNFError e s) (CNFColAggr e s)
    specializeColAggr = either (const err) return . cnfSpecializeQueryF

ncnfLiftC2 :: forall e s m c1 c2 c3 x .
             (Bifunctor c1,Bifunctor c2,Bifunctor c3, Functor m,
              Hashables2 e s,
              HashableCNFC c1 [] e s,HashableCNFC c1 HashBag e s,
              HashableCNFC c2 [] e s,HashableCNFC c2 HashBag e s,
              HashableCNFC c3 [] e s,HashableCNFC c3 HashBag e s) =>
            (NCNFQueryCF c1 [] e s ->
             NCNFQueryCF c2 [] e s ->
             m (x,NCNFQueryCF c3 [] e s))
          -> NCNFQueryCF c1 HashBag e s
          -> NCNFQueryCF c2 HashBag e s
          -> m (x,NCNFQueryCF c3 HashBag e s)
ncnfLiftC2 f l r = fmap3 dropLi $ f (putLi <$> l) (putLi <$> r) where
  dropLi :: forall c . (HashableCNFC c [] e s, HashableCNFC c HashBag e s, Bifunctor c) =>
           CNFQueryCF c [] e s -> CNFQueryCF c HashBag e s
  dropLi = cnfMapColumns $ bimap toHashBag $ first toHashBag
  putLi :: forall c . (HashableCNFC c [] e s, HashableCNFC c HashBag e s,Bifunctor c) =>
          CNFQueryCF c HashBag e s -> CNFQueryCF c [] e s
  putLi = cnfMapColumns $ bimap toList $ first toList

ncnfLiftC1 :: forall e s m c1 c2 .
             (Bifunctor c1, Bifunctor c2, Functor m, Hashables2 e s,
              HashableCNFC c1 [] e s,HashableCNFC c1 HashBag e s,
              HashableCNFC c2 [] e s,HashableCNFC c2 HashBag e s) =>
             (NCNFQueryCF c1 [] e s -> m (NCNFQueryCF c2 [] e s))
           -> NCNFQueryCF c1 HashBag e s
           -> m (NCNFQueryCF c2 HashBag e s)
ncnfLiftC1 f ncnf = fmap2 dropLi $ f $ putLi <$> ncnf where
  dropLi :: CNFQueryCF c2 [] e s -> CNFQueryCF c2 HashBag e s
  dropLi = cnfMapColumns $ bimap toHashBag $ first toHashBag
  putLi :: CNFQueryCF c1 HashBag e s -> CNFQueryCF c1 [] e s
  putLi = cnfMapColumns $ bimap toList $ first toList


cnfAsProd :: (HashableCNF f' e s, HashableCNF f e s, Foldable f) =>
            CNFQueryF f e s -> CNFQueryCF EmptyF2 f' e s
cnfAsProd cnf = updateHash CNFQuery {
  cnfColumns=EmptyF2,
  cnfSel=mempty,
  cnfProd=bagSingleton $ HS.singleton $ Q0 $ Right $ runIdentity
    $ cnfTravColumns (Identity . bimap toHashBag (first toHashBag))
    cnf,
  cnfHash=undefined,
  cnfOrigDEBUG'=cnfOrigDEBUG' cnf
  }

cnfMapColumns :: (HashableCNFSPCF  sel_f prod_f col_f' f e s,
                 HashableCNFSPCF  sel_f prod_f col_f' f' e s) =>
                (col_f (CNFProj f e s) (CNFAggr f e s) ->
                  col_f' (CNFProj f' e s) (CNFAggr f' e s))
               -> CNFQuerySPDCF sel_f prod_f EmptyF col_f f e s
               -> CNFQuerySPDCF sel_f prod_f EmptyF col_f' f' e s
cnfMapColumns f = updateHashCol . cnfMapColumns' f
-- | Map but retain the hash. UNSAFE
cnfMapColumns' :: (HashableCNFSPCF  sel_f prod_f col_f' f e s,
                 HashableCNFSPCF  sel_f prod_f col_f' f' e s) =>
                 (col_f (CNFProj f e s) (CNFAggr f e s) ->
                  col_f' (CNFProj f' e s) (CNFAggr f' e s))
               -> CNFQuerySPDCF sel_f prod_f EmptyF col_f f e s
               -> CNFQuerySPDCF sel_f prod_f EmptyF col_f' f' e s
cnfMapColumns' f CNFQuery{..} = CNFQuery{
  cnfColumns=f cnfColumns,
  cnfSel=cnfSel,
  cnfProd=cnfProd,
  cnfHash=cnfHash,
  cnfOrigDEBUG'=EmptyF}
{-# SCC cnfMapColumns #-}
{-# INLINE cnfMapColumns #-}

cnfMapName :: forall c c' e s . HashableCNFC c' Identity e s =>
             (c (CNFProj Identity e s) (CNFAggr Identity e s) ->
              c' (CNFProj Identity e s) (CNFAggr Identity e s))
           -> CNFNameC c e s
           -> CNFNameC c' e s
cnfMapName f = \case
  Column c i -> Column (cnfMapColumns f c) i
  NonSymbolName e -> NonSymbolName e
  PrimaryCol e s i -> PrimaryCol e s i

aggrColToProjCol :: Hashables2 e s =>
                   CNFQueryAggr e s -> Expr (CNFColAggr e s) -> CNFColProj e s
aggrColToProjCol cnfAggr expr = updateHash CNFQuery {
  cnfColumns=
      Const $ Identity $ (`Column` 0) . cnfGeneralizeQueryF . Right <$> expr,
  cnfSel=mempty,
  cnfProd=
      bagSingleton $ HS.singleton
      $ Q0 $ Right $ cnfGeneralizeQueryF $ Right cnfAggr,
  cnfHash=undefined,
  cnfOrigDEBUG'=EmptyF
  }

-- | Wrap the aggregation CNF with a projection CNF.
ncnfAggrAsProj :: forall f e s .
                (Applicative f, Foldable f, HashableCNF f e s) =>
                NCNFQueryAggrF f e s -> NCNFQueryProjF f e s
ncnfAggrAsProj (nm,cnfAggr) = (nmProj,cnfProj)
  where
    cnfProj :: CNFQueryDCF EmptyF Const f e s
    cnfProj = updateHash CNFQuery {
      cnfColumns=Const cols,
      cnfSel=mempty,
      cnfProd=bagSingleton $ HS.singleton $ Q0 $ Right prod,
      cnfHash=(fst3 $ cnfHash cnfAggr,undefined,undefined),
      cnfOrigDEBUG'=cnfOrigDEBUG' cnfAggr
      }
    prod :: (CNFQueryI e s)
    prod = cnfMapColumns (bimap toHashBag $ first toHashBag)
           $ cnfGeneralizeQueryF $ Right cnfAggr
    nmProj :: NameMapProj e s
    nmProj = HM.map toProjCol nm where
      toProjCol :: CNFColC CoConst e s -> CNFColC Const e s
      toProjCol = cnfColAggrToProj
                   $ ((Identity . E0 . (`Column` 0) . fromJustErr)
                       ... flip lookup)
                   $ extractAggr
                   <$> toList cols
    extractAggr :: Expr (CNFName e2 s2)
                -> (Aggr (Expr (CNFName e2 s2)), CNFCol e2 s2)
    extractAggr = \case
      WrappedAggrColumn cnf ag -> (ag, cnf);
      _ -> error "cnfAggrAsProjCol ret val does not match WrappedAggrColumn pattern"
    cnfColAggrToProj :: (Aggr (Expr (CNFName e s)) -> CNFProj Identity e s)
                      -> CNFColAggr e s -> CNFColProj e s
    cnfColAggrToProj f = cnfMapColumns $ Const . f . runIdentity . fst . getCoConst
    cols :: CNFProj f e s
    cols = cnfAggrAsProjCol cnfAggr

-- |The expected pattern of all the proj wrapped columns.
pattern WrappedAggrColumn :: forall e s .
                            CNFCol e s
                          -> Aggr (Expr (CNFName e s))
                          -> Expr (CNFName e s)
pattern WrappedAggrColumn cnf aggr <-
  E0 (Column cnf@CNFQuery{cnfColumns=Right (Identity aggr,_)} 0)
cnfAggrAsProjCol :: forall f e s . (HashableCNF f e s, Applicative f) =>
                   CNFQueryAggrF f e s -> CNFProj f e s
cnfAggrAsProjCol cnf = colsProj
  where
    aggrColAsName :: CNFQueryAggrF Identity e s -> CNFName e s
    aggrColAsName = (`Column` 0) . cnfGeneralizeQueryF . Right
    colsProj :: f (Expr (CNFName e s))
    colsProj = E0 . aggrColAsName <$> cnfGetCols cnf

cnfGetCols :: (HashableCNFC c Identity e s,
              HashableCNFC c f e s,
              Bitraversable c, Applicative f) =>
             CNFQueryDCF EmptyF c f e s
           -> f (CNFQueryDCF EmptyF c Identity e s)
cnfGetCols = cnfTravColumns $ bitraverse toList' $ bitraverse toList' pure where
  toList' = fmap Identity

cnfIsColOf :: forall sel_f prod_f f e s .
             (HashableCNFSPCF sel_f prod_f Either Identity e s,
              HashableCNFSPCF sel_f prod_f Either f e s,
              Foldable f,
              Hashables2 e s) =>
             CNFQuerySPDCF sel_f prod_f EmptyF Either Identity e s
           -> CNFQuerySPDCF sel_f prod_f EmptyF Either f e s
           -> Bool
cnfIsColOf col cnf = uncolCol col == uncolQ cnf
                     && case (cnfColumns col, cnfColumns cnf) of
                         (Right (Identity x,_), Right (xs,_)) -> x `elem` xs
                         (Left (Identity x), Left xs) -> x `elem` xs
                         _ -> error "dropColumns equaliy makes this unreachable"
  where
    uncolCol :: CNFQuerySPDCF sel_f prod_f EmptyF Either Identity e s
             -> CNFQuerySPDCF sel_f prod_f EmptyF EmptyF2 EmptyF e s
    uncolCol = cnfDropColumnsC
    uncolQ :: CNFQuerySPDCF sel_f prod_f EmptyF Either f e s
           -> CNFQuerySPDCF sel_f prod_f EmptyF EmptyF2 EmptyF e s
    uncolQ = cnfDropColumnsC

cnfDropColumnsF :: forall c f e s .
                  (HashableCNFC c f e s,
                   HashableCNFC c EmptyF e s,
                   Bitraversable c) =>
                  CNFQueryCF c f e s -> CNFQueryCF c EmptyF e s
cnfDropColumnsF = cnfMapColumns $ bimap f (first f) where
  f = const EmptyF
cnfDropColumnsC :: (HashableCNFC col_f f' e s,
                   HashableCNFSPCF sel_f prod_f col_f f e s,
                   Bitraversable col_f) =>
                  CNFQuerySPDCF sel_f prod_f EmptyF col_f f e s
                -> CNFQuerySPDCF sel_f prod_f EmptyF EmptyF2 f' e s
cnfDropColumnsC = cnfMapColumns $ const EmptyF2

cnfTravColumns :: forall g sel_f prod_f col_f col_f' f f' e s .
                 (HashableCNFSPCF sel_f prod_f col_f' f e s,
                  HashableCNFSPCF sel_f prod_f col_f' f' e s,
                  Functor g) =>
                 (col_f (CNFProj f e s) (CNFAggr f e s) ->
                  g (col_f' (CNFProj f' e s) (CNFAggr f' e s)))
               -> CNFQuerySPDCF sel_f prod_f EmptyF col_f f e s
               -> g (CNFQuerySPDCF sel_f prod_f EmptyF col_f' f' e s)
cnfTravColumns f cnf = updateHashCol <$> cnfTravColumns' f cnf

cnfTravColumns' :: forall g sel_f prod_f col_f col_f' f f' e s .
                 (HashableCNFSPCF sel_f prod_f col_f' f e s,
                  HashableCNFSPCF sel_f prod_f col_f' f' e s,
                  Functor g) =>
                 (col_f (CNFProj f e s) (CNFAggr f e s) ->
                  g (col_f' (CNFProj f' e s) (CNFAggr f' e s)))
               -> CNFQuerySPDCF sel_f prod_f EmptyF col_f f e s
               -> g (CNFQuerySPDCF sel_f prod_f EmptyF col_f' f' e s)
cnfTravColumns' f cnf = onCols <$> f (cnfColumns cnf) where
  onCols :: col_f' (CNFProj f' e s) (CNFAggr f' e s)
         -> CNFQuerySPDCF sel_f prod_f EmptyF col_f' f' e s
  onCols cols = CNFQuery{
    cnfColumns=cols,
    cnfSel=cnfSel cnf,
    cnfProd=cnfProd cnf,
    cnfHash=cnfHash cnf,
    cnfOrigDEBUG'=cnfOrigDEBUG' cnf}
-- | Unsafe map. For maps that don't change the hash.
ncnfMapColumns' :: forall col_f col_f' f f' e s .
                  (HashableCNFSPCF HS.HashSet HashBag col_f' f e s,
                   HashableCNFSPCF HS.HashSet HashBag col_f' f' e s,
                   HashableCNFSPCF HS.HashSet HashBag col_f' Identity e s) =>
                  (col_f (CNFProj Identity e s) (CNFAggr Identity e s) ->
                   col_f' (CNFProj Identity e s) (CNFAggr Identity e s))
                -> (col_f (CNFProj f e s) (CNFAggr f e s) ->
                    col_f' (CNFProj f' e s) (CNFAggr f' e s))
                -> NCNFQueryDCF EmptyF col_f f e s
                -> NCNFQueryDCF EmptyF col_f' f' e s
ncnfMapColumns' fcol f (nm,cnf) = (cnfMapColumns' fcol <$> nm,cnfMapColumns' f cnf)

ncnfGeneralizeQueryF :: HashableCNF f e s =>
                       Either (NCNFQueryProjF f e s) (NCNFQueryAggrF f e s)
                     -> NCNFQueryF f e s
ncnfGeneralizeQueryF = either
  (ncnfMapColumns' (Left . getConst) $ Left . getConst)
  (ncnfMapColumns' (Right . getCoConst) $ Right . getCoConst)

cnfGeneralizeQueryF :: HashableCNF f e s =>
                      Either (CNFQueryProjF f e s) (CNFQueryAggrF f e s)
                    -> CNFQueryF f e s
cnfGeneralizeQueryF = either
  (cnfMapColumns' $ Left . getConst)
  (cnfMapColumns' $ Right . getCoConst)
cnfSpecializeQueryF :: forall f e s . HashableCNF f e s =>
                      CNFQueryF f e s
                    -> Either (CNFQueryProjF f e s) (CNFQueryAggrF f e s)
cnfSpecializeQueryF cnf = case cnfColumns cnf of
  Left x  -> Left $ mkQ $ Const x
  Right x -> Right $ mkQ $ CoConst x
  where
    mkQ :: forall c . HashableCNFC c f e s =>
          c (CNFProj f e s) (CNFAggr f e s)
        -> CNFQueryDCF EmptyF c f e s
    mkQ c = CNFQuery{
      cnfColumns=c,
      cnfSel=cnfSel cnf,
      cnfProd=cnfProd cnf,
      cnfHash=cnfHash cnf,
      cnfOrigDEBUG'=EmptyF}
ncnfSpecializeQueryF :: forall f e s . HashableCNF f e s =>
                       NCNFQueryF f e s
                     -> Either (CNFError e s )
                     (Either (NCNFQueryProjF f e s) (NCNFQueryAggrF f e s))
ncnfSpecializeQueryF (nm,cnf) = case cnfSpecializeQueryF cnf of
  Left projCnf -> Left . (,projCnf) <$> HM.traverseWithKey (const go) nm where
    go :: CNFCol e s
       -> Either (CNFError e s) (CNFColProj e s)
    go = cnfTravColumns' $ \case
      Left a -> return $ Const a
      Right _ -> Left $ CNFErrorMsg "Mismatch: CNF is proj, namemap refers to aggr"
  Right aggrCnf -> Right . (,aggrCnf) <$> HM.traverseWithKey (const go) nm where
    go = cnfTravColumns' $ \case
      Right a -> return $ CoConst a
      Left _ -> Left $ CNFErrorMsg "Mismatch: CNF is aggr, namemap refers to proj"
ncnfToCnf :: NCNFQuery e s -> CNFQuery e s
ncnfToCnf = snd
ncnfModQueryDebug :: (d (Query e s) -> d' (Query e s))
                  -> NCNFQueryDCF d c f e s
                  -> NCNFQueryDCF d' c f e s
ncnfModQueryDebug f (nm,cnf) = (nm,cnf{cnfOrigDEBUG'=f $ cnfOrigDEBUG' cnf})

-- |Assuming the cnf has been updated internally, update the namemap
-- too.
sanitizeNameMap :: forall d c f e s .
                  (Hashables2 e s, HashableCNFC c Identity e s) =>
                  NCNFQueryDCF d c f e s
                -> ([((e,CNFColC c e s),(e,CNFColC c e s))],
                   NCNFQueryDCF d c f e s)
sanitizeNameMap (nm,cnf) = -- assertSane
  (toList nmPairs, (snd . snd <$> nmPairs,cnf))
  where
    nmPairs :: HM.HashMap e ((e, CNFColC c e s),(e, CNFColC c e s))
    nmPairs = HM.mapWithKey promoteCol nm where
      promoteCol :: a
                 -> CNFQueryDCF EmptyF c Identity e s
                 -> ((a,CNFQueryDCF EmptyF c Identity e s),
                    (a,CNFQueryDCF EmptyF c Identity e s))
      promoteCol e col = -- XXX: do we really need to update all hashes?
        ((e,col),(e,col{
                     cnfProd=cnfProd cnf,
                     cnfSel=cnfSel cnf,
                     cnfHash=(fst3 $ cnfHash col,
                              snd3 $ cnfHash cnf,
                              trd3 $ cnfHash cnf)}))

    assertSane x =
      if all (\c -> cnfSel cnf == cnfSel c && cnfProd c == cnfProd cnf) outColumns
      then x
      else error "bad sanitization of namemaps"
    outColumns = snd . snd <$> toList nmPairs

chainAssoc :: Eq b => [(a,b)] -> [(b,c)] -> Maybe [(a,c)]
chainAssoc ab bc = sequenceA [(a,) <$> lookup b bc | (a,b) <- ab]
