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

module Data.QnfQuery.BuildProduct
  ( nqnfProduct
  , nqnfProduct0
  , incrementQNF
  , incrementCol
  ) where

import           Control.Applicative      hiding (Const (..))
import           Control.Monad.Except
import           Control.Monad.Morph
import           Control.Monad.State
import           Data.Bifunctor
import           Data.QnfQuery.BuildUtils
import           Data.QnfQuery.HashBag
import           Data.QnfQuery.Types
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

nqnfProduct :: forall e s. Hashables2 e s =>
              NQNFQueryI e s
            -> NQNFQueryI e s
            -> ListT (QNFBuild e s) (NQNFResultI () e s)
#ifdef NO_CACHE
nqnfProduct l r = hoist lift $ nqnfProductI l r
#endif
#ifdef NAIVE_CACHE
nqnfProduct l r = do
  cache <- qnfProduct_cache <$> get
  let lrLu = HM.lookup (l,r) cache
  let rlLu = HM.lookup (r,l) cache
  case lrLu <|> rlLu of
    Just a  -> hoist lift a
    Nothing -> do
      let ret = nqnfProductI l r
      modify $
        \cc -> cc{qnfProduct_cache=HM.insert (l,r) ret $ qnfProduct_cache cc}
      hoist lift ret
#endif
#ifdef COLUMN_CACHE
nqnfProduct l@(nml,_) r@(nmr,_) = do
  -- XXX:  Cache drop selections.
  -- Note that if we have a name conflict the query was invalid to begin with.
  HM.lookup key . qnfProduct_cache' <$> get >>= \case
    Just a  -> hoist lift a
    Nothing -> do
      let ret = nqnfProductI l r
      modify $
        \cc -> cc{qnfProduct_cache'=HM.insert key ret $ qnfProduct_cache' cc}
      hoist lift ret
  where
    key = nml <> nmr
#endif

nqnfProductI :: forall e s. Hashables2 e s =>
               NQNFQueryI e s
             -> NQNFQueryI e s
             -> ListT (Either (QNFError e s)) (NQNFResultI () e s)
nqnfProductI l r = go <$> nqnfProduct0 l r where
  go (ioAssoc,nqnf) = NQNFResult {
    nqnfResNQNF=nqnf,
    nqnfResOrig=(),
    nqnfResInOutNames=fmap (bimap gen gen) ioAssoc
    }
  gen = fmap (qnfGeneralizeQueryF . Left)

nqnfProduct0 :: forall e s. Hashables2 e s =>
              NQNFQueryI e s
            -> NQNFQueryI e s
            -> ListT (Either (QNFError e s))
            ([((e,QNFColProj e s), (e,QNFColProj e s))], NQNFQueryI e s)
nqnfProduct0 = nqnfLiftC2 nqnfProductAp

-- | Return a list of valid namemaps from the union of these two
-- namemaps. Either offset the left namemap by the right bag's
-- multiplicities (making the right namemap have lower offsets on
-- collisions) or visa versa. We assume there are no name collisions
-- in between the twoo namemaps.
nqnfProductAp :: forall f e s .
                (Applicative f, Foldable f,
                 HashableQNF f e s,
                 Semigroup (QNFProj f e s),
                 Eq (f (Expr (QNFName e s))),
                 Eq (f (Aggr (Expr (QNFName e s))))) =>
                NQNFQueryF f e s
              -> NQNFQueryF f e s
              -> ListT (Either (QNFError e s))
              ([((e,QNFColProj e s),(e,QNFColProj e s))], NQNFQueryF f e s)
nqnfProductAp nqnfl nqnfr = do
  Tup2 nqnflPrj nqnfrPrj <- traverse (lift . nqnfAsProj) $ Tup2 nqnfl nqnfr
  Tup2 (ml,nqnfl') (mr,nqnfr') <- incrementedQnfs nqnflPrj nqnfrPrj
  return $ bimap
    (remapAssoc $ ml <> mr)
    (bimap generalizeNameMap (qnfGeneralizeQueryF . Left))
    $ nqnfProductUnsafe nqnfl' nqnfr'
  where
    incrementedQnfs :: NQNFQueryProjF f e s
                    -> NQNFQueryProjF f e s
                    -> ListT (Either (QNFError e s))
                    (Tup2 ([((e, QNFColProj e s),(e, QNFColProj e s))],
                            NQNFQueryProjF f e s))
    incrementedQnfs nqnfl0 nqnfr0 = if not $ bagIntersects pl pr
        then return $ Tup2 ([],nqnfl0) ([],nqnfr0)
        else
          (lift $ (`Tup2` ([],nqnfr0)) <$> nqnfIncrementProds pr nqnfl0)
          <|> (lift $ Tup2 ([],nqnfl0) <$> nqnfIncrementProds pl nqnfr0)
      where
        Tup2 pl pr = qnfProd . snd <$> Tup2 nqnfl0 nqnfr0
    remapAssoc :: [((e, QNFColProj e s), (e, QNFColProj e s))]
               -> [((e, QNFColProj e s), (e, QNFColProj e s))]
               -> [((e, QNFColProj e s), (e, QNFColProj e s))]
    remapAssoc assc target =
      [(fromMaybe from $ lookup from (fmap swap assc),to) | (from,to) <- target]
    generalizeNameMap :: NameMapProj e s -> NameMap e s
    generalizeNameMap = HM.map $ qnfMapColumns $ Left . getConst

nqnfProductUnsafe :: (Semigroup (QNFProj f e s), HashableQNF f e s) =>
                    NQNFQueryProjF f e s
                  -> NQNFQueryProjF f e s
                  -> ([((e,QNFColProj e s),(e,QNFColProj e s))],
                     NQNFQueryProjF f e s)
nqnfProductUnsafe (nm,qnf) (nm',qnf') = sanitizeNameMap
  (nm `unionSafe` nm',
    updateHash QNFQuery {
      qnfColumns=Const $ getConst (qnfColumns qnf) <> getConst  (qnfColumns qnf'),
      qnfSel=qnfSel qnf <> qnfSel qnf',
      qnfProd=check $ qnfProd qnf <> qnfProd qnf',
      qnfOrigDEBUG'=EmptyF,
      qnfHash=undefined})
  where
    unionSafe l r = if HM.null $ HM.intersection l r
      then l <> r
      else error "Conflicting names! Nothing can be correct now."
    check res = if bagNull $ qnfProd qnf'
      then error "L Same in as out"
      else res

alignMap :: Eq a => [(a,b)] -> [(a,b)] -> Either (QNFError e s) [((a,b),(a,b))]
alignMap l r = runListT $ do
  (a,b) <- mkListT $ return l
  case a `lookup` r of
    Nothing -> throwAStr "Lookup error"
    Just b' -> return ((a,b),(a,b'))

-- | In sel/proj of the NQNF increment the terms found in a bag. Use
-- this to mix sel/proj of of conflicting products.
nqnfIncrementProds
  :: forall f e s .
  (Foldable f,Functor f,HashableQNF f e s)
  => HashBag (QNFProd e s)
  -> NQNFQueryProjF f e s
  -> Either
    (QNFError e s)
    ([((e,QNFColProj e s),(e,QNFColProj e s))],NQNFQueryProjF f e s)
nqnfIncrementProds bag nqnf@(nm,qnf) = do
  assoc <- alignMap symAssocBefore symAssocAfter
  return (assoc,nqnf')
  where
    Tup2 symAssocBefore symAssocAfter = nqnfToSymAssoc <$> Tup2 nqnf nqnf'
    nqnf' = (incrementQNF bag <$> nm,incrementQNF bag qnf)
    nqnfToSymAssoc :: NQNFQueryProjF f e s -> [(e, QNFColProj e s)]
    nqnfToSymAssoc = fmap fst . fst . sanitizeNameMap

incrementQNF :: forall f e s . (Functor f, HashableQNF f e s) =>
               HashBag (QNFProd e s)
             -> QNFQueryProjF f e s
             -> QNFQueryProjF f e s
incrementQNF bag qnf' = updateHashSel $ updateHashCol qnf'{
  qnfColumns=mapProjAggr (qnfNameIncrementProdsE bag) $ qnfColumns qnf',
  qnfSel=HS.map (fmap4 $ incrementCol bag) $ qnfSel qnf',
  qnfHash=qnfHash qnf'}

incrementCol :: Hashables2 e s =>
          HashBag (QNFProd e s)
        -> QNFQuerySPDCF EmptyF EmptyF EmptyF Const Identity e s
        -> QNFQuerySPDCF EmptyF EmptyF EmptyF Const Identity e s
incrementCol bag = qnfMapColumns $ first $ fmap2 $ qnfNameIncrementProdsE bag

mapProjAggr :: (Bifunctor c, Functor f, Hashables2 e s) =>
              (QNFName e s -> QNFName e s)
            -> c (QNFProj f e s) (QNFAggr f e s)
            -> c (QNFProj f e s) (QNFAggr f e s)
mapProjAggr f = bimap (fmap2 f) (bimap (fmap3 f) (HS.map $ fmap f))

type SelProdClass s p = (s ~ HS.HashSet, p ~ HashBag)
-- | If a prod in the bag matches the column of the name increment by
-- it's multiplicity.
-- Look for a qnf in product that matches and stop when (if) you find it.
qnfNameIncrementProdsE :: forall e s qnf_name sel_f prod_f .
                         (Hashables2 e s,
                          SelProdClass sel_f prod_f,
                          qnf_name ~ QNFNameSPC sel_f prod_f Either) =>
                        HashBag (QNFProd e s) -> qnf_name e s -> qnf_name e s
qnfNameIncrementProdsE bag =
  (`evalState` False) . foldr go return (HM.toList $ unHashBag bag)
  where
    go :: (QNFProd e s,Int)
       -> (qnf_name e s -> State Bool (qnf_name e s))
       -> qnf_name e s -> State Bool (qnf_name e s)
    go (_,0) _ _ = error "what?"
    go (q,i) rest n = do
      newName <- qnfNameIncrementProd (\x -> put True >> return (i + x)) q n
      get >>= \case
        True -> return newName
        False -> rest n

-- | Look for a qnf in product that matches and stop when (if) you find it.
qnfNameIncrementProd :: forall e s f sel_f prod_f .
                       (Monad f, SelProdClass sel_f prod_f, Hashables2 e s) =>
                       (Int -> f Int)
                     -> QNFProd e s
                     -> QNFNameSPC sel_f prod_f Either e s
                     -> f (QNFNameSPC sel_f prod_f Either e s)
qnfNameIncrementProd f qs = (`evalStateT` False) . foldr go return qs where
  go :: Query (QNFName e s) (Either s (QNFQueryI e s))
     -> (QNFNameSPC sel_f prod_f Either e s ->
        StateT Bool f (QNFNameSPC sel_f prod_f Either e s))
     -> QNFNameSPC sel_f prod_f Either e s
     -> StateT Bool f (QNFNameSPC sel_f prod_f Either e s)
  go q rest n = do
    newName <- qnfNameIncrementProd1 (\x -> put True >> lift (f x)) q n
    get >>= \case
      True -> return newName
      False -> rest n

-- | When name matches increment it.
qnfNameIncrementProd1 :: forall e s sel_f prod_f f .
                        (Applicative f, SelProdClass sel_f prod_f,
                         Hashables2 e s) =>
                        (Int -> f Int)
                      -> Query (QNFName e s) (Either s (QNFQueryI e s))
                      -> QNFNameSPC sel_f prod_f Either e s
                      -> f (QNFNameSPC sel_f prod_f Either e s)
qnfNameIncrementProd1 f = \case
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
    recur = qnfNameIncrementProd1 f
    fin = qnfNameIncrementAtom f

-- | If the name refers to something in the bag increment by the
-- multiplicity in the bag.
qnfNameIncrementAtom :: (Applicative f,
                        SelProdClass sel_f prod_f,
                        Hashables2 e s) =>
                       (Int -> f Int)
                     -> Either s (QNFQueryI e s)
                     -> QNFNameSPC sel_f prod_f Either e s
                     -> f (QNFNameSPC sel_f prod_f Either e s)
qnfNameIncrementAtom f = \case
  Left s -> \case
    x@(PrimaryCol e s' i) -> if s == s' then PrimaryCol e s' <$> f i else pure x
    x -> pure x
  Right qnf -> \case
    x@(Column col i) -> if col `qnfIsColOf` qnf
      then Column col <$> f i
      else pure x
    x -> pure x
