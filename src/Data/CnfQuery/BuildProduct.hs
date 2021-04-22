{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# OPTIONS_GHC -Wno-simplifiable-class-constraints #-}

-- | Normalize queries for comparison

module Data.CnfQuery.BuildProduct
  ( ncnfProduct
  , ncnfProduct0
  , incrementCNF
  , incrementCol
  ) where

import           Control.Applicative      hiding (Const (..))
import           Control.Monad.Except
import           Control.Monad.Morph
import           Control.Monad.State
import           Data.Bifunctor
import           Data.CnfQuery.BuildUtils
import           Data.CnfQuery.HashBag
import           Data.CnfQuery.Types
import           Data.Functor.Identity
import qualified Data.HashMap.Lazy        as HM
import qualified Data.HashSet             as HS
import           Data.Maybe
import           Data.Query.Algebra
import           Data.Utils.AShow
import           Data.Utils.Const
import           Data.Utils.EmptyF
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.Tup

#define NO_CACHE
-- #define NAIVE_CACHE
-- #define COLUMN_CACHE

ncnfProduct :: forall e s. Hashables2 e s =>
              NCNFQueryI e s
            -> NCNFQueryI e s
            -> ListT (CNFBuild e s) (NCNFResultI () e s)
#ifdef NO_CACHE
ncnfProduct l r = hoist lift $ ncnfProductI l r
#endif
#ifdef NAIVE_CACHE
ncnfProduct l r = do
  cache <- cnfProduct_cache <$> get
  let lrLu = HM.lookup (l,r) cache
  let rlLu = HM.lookup (r,l) cache
  case lrLu <|> rlLu of
    Just a  -> hoist lift a
    Nothing -> do
      let ret = ncnfProductI l r
      modify $
        \cc -> cc{cnfProduct_cache=HM.insert (l,r) ret $ cnfProduct_cache cc}
      hoist lift ret
#endif
#ifdef COLUMN_CACHE
ncnfProduct l@(nml,_) r@(nmr,_) = do
  -- XXX:  Cache drop selections.
  -- Note that if we have a name conflict the query was invalid to begin with.
  HM.lookup key . cnfProduct_cache' <$> get >>= \case
    Just a  -> hoist lift a
    Nothing -> do
      let ret = ncnfProductI l r
      modify $
        \cc -> cc{cnfProduct_cache'=HM.insert key ret $ cnfProduct_cache' cc}
      hoist lift ret
  where
    key = nml <> nmr
#endif

ncnfProductI :: forall e s. Hashables2 e s =>
               NCNFQueryI e s
             -> NCNFQueryI e s
             -> ListT (Either (CNFError e s)) (NCNFResultI () e s)
ncnfProductI l r = go <$> ncnfProduct0 l r where
  go (ioAssoc,ncnf) = NCNFResult {
    ncnfResNCNF=ncnf,
    ncnfResOrig=(),
    ncnfResInOutNames=fmap (bimap gen gen) ioAssoc
    }
  gen = fmap (cnfGeneralizeQueryF . Left)

ncnfProduct0 :: forall e s. Hashables2 e s =>
              NCNFQueryI e s
            -> NCNFQueryI e s
            -> ListT (Either (CNFError e s))
            ([((e,CNFColProj e s), (e,CNFColProj e s))], NCNFQueryI e s)
ncnfProduct0 = ncnfLiftC2 ncnfProductAp

-- | Return a list of valid namemaps from the union of these two
-- namemaps. Either offset the left namemap by the right bag's
-- multiplicities (making the right namemap have lower offsets on
-- collisions) or visa versa. We assume there are no name collisions
-- in between the twoo namemaps.
ncnfProductAp :: forall f e s .
                (Applicative f, Foldable f,
                 HashableCNF f e s,
                 Semigroup (CNFProj f e s),
                 Eq (f (Expr (CNFName e s))),
                 Eq (f (Aggr (Expr (CNFName e s))))) =>
                NCNFQueryF f e s
              -> NCNFQueryF f e s
              -> ListT (Either (CNFError e s))
              ([((e,CNFColProj e s),(e,CNFColProj e s))], NCNFQueryF f e s)
ncnfProductAp ncnfl ncnfr = do
  Tup2 ncnflPrj ncnfrPrj <- traverse (lift . ncnfAsProj) $ Tup2 ncnfl ncnfr
  Tup2 (ml,ncnfl') (mr,ncnfr') <- incrementedCnfs ncnflPrj ncnfrPrj
  return $ bimap
    (remapAssoc $ ml <> mr)
    (bimap generalizeNameMap (cnfGeneralizeQueryF . Left))
    $ ncnfProductUnsafe ncnfl' ncnfr'
  where
    incrementedCnfs :: NCNFQueryProjF f e s
                    -> NCNFQueryProjF f e s
                    -> ListT (Either (CNFError e s))
                    (Tup2 ([((e, CNFColProj e s),(e, CNFColProj e s))],
                            NCNFQueryProjF f e s))
    incrementedCnfs ncnfl0 ncnfr0 = if not $ bagIntersects pl pr
        then return $ Tup2 ([],ncnfl0) ([],ncnfr0)
        else
          (lift $ (`Tup2` ([],ncnfr0)) <$> ncnfIncrementProds pr ncnfl0)
          <|> (lift $ Tup2 ([],ncnfl0) <$> ncnfIncrementProds pl ncnfr0)
      where
        Tup2 pl pr = cnfProd . snd <$> Tup2 ncnfl0 ncnfr0
    remapAssoc :: [((e, CNFColProj e s), (e, CNFColProj e s))]
               -> [((e, CNFColProj e s), (e, CNFColProj e s))]
               -> [((e, CNFColProj e s), (e, CNFColProj e s))]
    remapAssoc assc target =
      [(fromMaybe from $ lookup from (fmap swap assc),to) | (from,to) <- target]
    generalizeNameMap :: NameMapProj e s -> NameMap e s
    generalizeNameMap = HM.map $ cnfMapColumns $ Left . getConst

ncnfProductUnsafe :: (Semigroup (CNFProj f e s), HashableCNF f e s) =>
                    NCNFQueryProjF f e s
                  -> NCNFQueryProjF f e s
                  -> ([((e,CNFColProj e s),(e,CNFColProj e s))],
                     NCNFQueryProjF f e s)
ncnfProductUnsafe (nm,cnf) (nm',cnf') = sanitizeNameMap
  (nm `unionSafe` nm',
    updateHash CNFQuery {
      cnfColumns=Const $ getConst (cnfColumns cnf) <> getConst  (cnfColumns cnf'),
      cnfSel=cnfSel cnf <> cnfSel cnf',
      cnfProd=check $ cnfProd cnf <> cnfProd cnf',
      cnfOrigDEBUG'=EmptyF,
      cnfHash=undefined})
  where
    unionSafe l r = if HM.null $ HM.intersection l r
      then l <> r
      else error "Conflicting names! Nothing can be correct now."
    check res = if bagNull $ cnfProd cnf'
      then error "L Same in as out"
      else res

alignMap :: Eq a => [(a,b)] -> [(a,b)] -> Either (CNFError e s) [((a,b),(a,b))]
alignMap l r = runListT $ do
  (a,b) <- mkListT $ return l
  case a `lookup` r of
    Nothing -> throwAStr "Lookup error"
    Just b' -> return ((a,b),(a,b'))

-- | In sel/proj of the NCNF increment the terms found in a bag. Use
-- this to mix sel/proj of of conflicting products.
ncnfIncrementProds
  :: forall f e s .
  (Foldable f,Functor f,HashableCNF f e s)
  => HashBag (CNFProd e s)
  -> NCNFQueryProjF f e s
  -> Either
    (CNFError e s)
    ([((e,CNFColProj e s),(e,CNFColProj e s))],NCNFQueryProjF f e s)
ncnfIncrementProds bag ncnf@(nm,cnf) = do
  assoc <- alignMap symAssocBefore symAssocAfter
  return (assoc,ncnf')
  where
    Tup2 symAssocBefore symAssocAfter = ncnfToSymAssoc <$> Tup2 ncnf ncnf'
    ncnf' = (incrementCNF bag <$> nm,incrementCNF bag cnf)
    ncnfToSymAssoc :: NCNFQueryProjF f e s -> [(e, CNFColProj e s)]
    ncnfToSymAssoc = fmap fst . fst . sanitizeNameMap

incrementCNF :: forall f e s . (Functor f, HashableCNF f e s) =>
               HashBag (CNFProd e s)
             -> CNFQueryProjF f e s
             -> CNFQueryProjF f e s
incrementCNF bag cnf' = updateHashSel $ updateHashCol cnf'{
  cnfColumns=mapProjAggr (cnfNameIncrementProdsE bag) $ cnfColumns cnf',
  cnfSel=HS.map (fmap4 $ incrementCol bag) $ cnfSel cnf',
  cnfHash=cnfHash cnf'}

incrementCol :: Hashables2 e s =>
          HashBag (CNFProd e s)
        -> CNFQuerySPDCF EmptyF EmptyF EmptyF Const Identity e s
        -> CNFQuerySPDCF EmptyF EmptyF EmptyF Const Identity e s
incrementCol bag = cnfMapColumns $ first $ fmap2 $ cnfNameIncrementProdsE bag

mapProjAggr :: (Bifunctor c, Functor f, Hashables2 e s) =>
              (CNFName e s -> CNFName e s)
            -> c (CNFProj f e s) (CNFAggr f e s)
            -> c (CNFProj f e s) (CNFAggr f e s)
mapProjAggr f = bimap (fmap2 f) (bimap (fmap3 f) (HS.map $ fmap f))

type SelProdClass s p = (s ~ HS.HashSet, p ~ HashBag)
-- | If a prod in the bag matches the column of the name increment by
-- it's multiplicity.
-- Look for a cnf in product that matches and stop when (if) you find it.
cnfNameIncrementProdsE :: forall e s cnf_name sel_f prod_f .
                         (Hashables2 e s,
                          SelProdClass sel_f prod_f,
                          cnf_name ~ CNFNameSPC sel_f prod_f Either) =>
                        HashBag (CNFProd e s) -> cnf_name e s -> cnf_name e s
cnfNameIncrementProdsE bag =
  (`evalState` False) . foldr go return (HM.toList $ unHashBag bag)
  where
    go :: (CNFProd e s,Int)
       -> (cnf_name e s -> State Bool (cnf_name e s))
       -> cnf_name e s -> State Bool (cnf_name e s)
    go (_,0) _ _ = error "what?"
    go (q,i) rest n = do
      newName <- cnfNameIncrementProd (\x -> put True >> return (i + x)) q n
      get >>= \case
        True -> return newName
        False -> rest n

-- | Look for a cnf in product that matches and stop when (if) you find it.
cnfNameIncrementProd :: forall e s f sel_f prod_f .
                       (Monad f, SelProdClass sel_f prod_f, Hashables2 e s) =>
                       (Int -> f Int)
                     -> CNFProd e s
                     -> CNFNameSPC sel_f prod_f Either e s
                     -> f (CNFNameSPC sel_f prod_f Either e s)
cnfNameIncrementProd f qs = (`evalStateT` False) . foldr go return qs where
  go :: Query (CNFName e s) (Either s (CNFQueryI e s))
     -> (CNFNameSPC sel_f prod_f Either e s ->
        StateT Bool f (CNFNameSPC sel_f prod_f Either e s))
     -> CNFNameSPC sel_f prod_f Either e s
     -> StateT Bool f (CNFNameSPC sel_f prod_f Either e s)
  go q rest n = do
    newName <- cnfNameIncrementProd1 (\x -> put True >> lift (f x)) q n
    get >>= \case
      True -> return newName
      False -> rest n

-- | When name matches increment it.
cnfNameIncrementProd1 :: forall e s sel_f prod_f f .
                        (Applicative f, SelProdClass sel_f prod_f,
                         Hashables2 e s) =>
                        (Int -> f Int)
                      -> Query (CNFName e s) (Either s (CNFQueryI e s))
                      -> CNFNameSPC sel_f prod_f Either e s
                      -> f (CNFNameSPC sel_f prod_f Either e s)
cnfNameIncrementProd1 f = \case
  Q2 o l r -> case o of
    QJoin _          -> error "Shouldn't have reached"
    QProd            -> error "Shouldn't have reached"
    QProjQuery       -> recur l
    QUnion           -> recur l
    QDistinct        -> error "Look up what distinct means"
    QLeftAntijoin _  -> recur l
    QRightAntijoin _ -> recur r
  Q1 o q -> case o of
    QSel  _    -> error "Shouldn't have reached"
    QGroup _ _ -> error "Shouldn't have reached"
    QProj _    -> error "Shouldn't have reached"
    QSort _    -> recur q
    QLimit _   -> recur q
    QDrop _    -> recur q
  Q0 atomFin -> fin atomFin
  where
    recur = cnfNameIncrementProd1 f
    fin = cnfNameIncrementAtom f

-- | If the name refers to something in the bag increment by the
-- multiplicity in the bag.
cnfNameIncrementAtom :: (Applicative f,
                        SelProdClass sel_f prod_f,
                        Hashables2 e s) =>
                       (Int -> f Int)
                     -> Either s (CNFQueryI e s)
                     -> CNFNameSPC sel_f prod_f Either e s
                     -> f (CNFNameSPC sel_f prod_f Either e s)
cnfNameIncrementAtom f = \case
  Left s -> \case
    x@(PrimaryCol e s' i) -> if s == s' then PrimaryCol e s' <$> f i else pure x
    x -> pure x
  Right cnf -> \case
    x@(Column col i) -> if col `cnfIsColOf` cnf
      then Column col <$> f i
      else pure x
    x -> pure x
