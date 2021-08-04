{-# LANGUAGE CPP #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE KindSignatures        #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MagicHash             #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}

-- | Normalize queries for comparison

module Data.QnfQuery.Types
  (QNFError(..)
  ,QNFCache(..)
  ,QNFBuild
  ,QNFSelName
  ,QNFNameSPC(..)
  ,QNFColSPC
  ,QNFNameC
  ,QNFQueryDCF
  ,QNFQuerySPDCF(..)
  ,QNFQueryCF
  ,QNFName
  ,QNFNameAggr
  ,QNFNameProj
  ,QNFQueryAggrF
  ,QNFQueryProjF
  ,QNFProd
  ,QNFSel
  ,QNFProj
  ,QNFAggr
  ,NameMap
  ,NameMapC
  ,NameMapAggr
  ,NameMapProj
  ,NQNFQuery
  ,NQNFQueryI
  ,NQNFQueryCF
  ,NQNFQueryDCF
  ,NQNFQueryF
  ,NQNFQueryAggrF
  ,NQNFQueryProjF
  ,QNFQueryAggr
  ,QNFQueryProj
  ,QNFQuery
  ,QNFQueryI
  ,QNFQueryF
  ,QNFCol
  ,QNFColC
  ,QNFColProj
  ,QNFColAggr
  ,QNFConstr
  ,QNFSemigroup(..)
  ,NQNFResultDF(..)
  ,NQNFResultF
  ,NQNFResultI
  ,NQNFResult
  ,HashableQNF
  ,HashableQNFSPCF
  ,HashableQNFC
  ,nameMap
  ,updateHash
  ,updateHashProd
  ,updateHashSel
  ,updateHashCol
  ,qnfCached) where

import Data.Utils.Function
import Data.Utils.Tup
import Data.Utils.ListT
import           Control.Monad.Identity
import           Control.Monad.State
import qualified Data.HashMap.Strict              as HM
import qualified Data.HashSet                     as HS
import           Data.Kind
import           Data.Query.Algebra
import           Data.Utils.AShow
import           Data.QnfQuery.HashBag
import           Data.Utils.Default
import           Data.Utils.EmptyF
import           Data.Utils.Const
import           Data.Utils.Hashable
import           GHC.Generics

data QNFError e s = QNFErrorMsg (AShowStr e s)
  | QNFProjNameMapOverAggrQnf
  | QNFUnknownSymbol e [e]
  | QNFSchemaMismatch [e] [e]
  | QNFMissingSchema (Query e s) s
  deriving (Eq, Generic)
instance AShowError e s (QNFError e s)
instance (AShowV e, AShowV s) => AShow (QNFError e s)
data QNFNameSPC sel_f prod_f col_f e s
  = PrimaryCol e s Int
  | Column (QNFColSPC sel_f prod_f col_f e s) Int
    -- ^ a a column c that refers to a:
    --
    -- QNF[P[c],S[c],Prod[Query[a ~ QNF]]]
  | NonSymbolName e
     -- ^ A literal.
  deriving Generic
type QNFName = QNFNameC Either
type QNFNameAggr = QNFNameC CoConst
type QNFNameProj = QNFNameC  Const
type QNFNameC = QNFNameSPC HS.HashSet HashBag
instance Default e => Default (QNFNameSPC sel_f prod_f col_f e s) where
  def = NonSymbolName def
instance QNFConstr AShow sel_f prod_f col_f Identity e s =>
  AShow (QNFNameSPC sel_f prod_f col_f e s)
  where
    ashow' = \case
      PrimaryCol e s i -> sexp "PrimaryCol" [ashow' e,ashow' s,ashow' i]
      Column c i -> sexp "Column" [ashow' $ hash c,ashow' i]
      NonSymbolName e -> sexp "NonSymbolName" [ashow' e]

instance (Hashables2 e s,QNFConstr ARead sel_f prod_f col_f Identity e s)
  => ARead (QNFNameSPC sel_f prod_f col_f e s)
instance (QNFConstr Eq sel_f prod_f col_f Identity e s,
          QNFConstr Hashable sel_f prod_f col_f Identity e s) =>
         Hashable (QNFNameSPC sel_f prod_f col_f e s)
instance QNFConstr AShow sel_f prod_f col_f Identity e s =>
         Show (QNFNameSPC sel_f prod_f col_f e s) where
  show = gshow
instance QNFConstr Eq sel_f prod_f col_f Identity e s =>
         Eq (QNFNameSPC sel_f prod_f col_f e s) where
  (==) = curry $ \case
    (PrimaryCol e s i, PrimaryCol e' s' i') -> e == e' && s == s' && i == i'
    (Column c i, Column c' i') -> c == c' && i == i'
    (NonSymbolName e, NonSymbolName e') -> e == e'
    _ -> False

-- | All the information required to match a QNFName to a
-- corresponding table.
type NameMapC c e s = HM.HashMap e (QNFColC c e s)
type NameMapAggr e s = NameMapC CoConst e s
type NameMapProj e s = NameMapC Const e s
type NameMap e s = NameMapC Either e s
type NQNFQueryDCF d c f e s = (NameMapC c e s, QNFQueryDCF d c f e s)
nameMap :: NQNFQueryDCF d c f e s -> NameMapC c e s
nameMap = fst
{-# INLINE nameMap #-}
type NQNFQueryCF c f e s = NQNFQueryDCF EmptyF c f e s
type NQNFQuery e s = NQNFQueryDCF Identity Either HashBag e s
-- A qnf query without the qnfQueryDebug
type NQNFQueryI e s = NQNFQueryDCF EmptyF Either HashBag e s
type NQNFQueryF f e s = NQNFQueryCF Either f e s
type NQNFQueryProjF f e s = NQNFQueryCF Const f e s
type NQNFQueryAggrF f e s = NQNFQueryCF CoConst f e s
type QNFColSPC s p c = QNFQuerySPDCF s p EmptyF c Identity
type QNFColC (c :: * -> * -> *) = (QNFQueryCF c Identity :: * -> * -> *)
type QNFCol = QNFColC Either
type QNFColProj = QNFColC Const
type QNFColAggr = QNFColC CoConst
-- Invariant: The column on Right refers to an E0, remember that
-- selections are under the aggregations and therefore can't be
_qnfSelNameSane :: QNFSelName e s -> Bool
_qnfSelNameSane = either (const True) $ projSane . qnfColumns where
  projSane = \case
    Const (Identity (E0 _)) -> True
    _ -> False

-- |Selection name refers to a version of the current QNF that has all
-- fields erased except the projection.
type QNFSelName e s = Either e (QNFQuerySPDCF EmptyF EmptyF EmptyF Const Identity e s)
type QNFProd e s = HS.HashSet (Query (QNFName e s) (Either s (QNFQueryI e s)))
type QNFSel e s =  Prop (Rel (Expr (QNFSelName e s)))
type QNFProj f e s = f (Expr (QNFName e s))
type QNFAggr f e s = (f (Aggr (QNFName e s)), HS.HashSet (Expr (QNFName e s)))
type QNFQueryI = QNFQueryF HashBag
type QNFQuery = QNFQueryDCF Identity Either HashBag
type QNFQueryF = QNFQueryCF Either
type QNFQueryAggrF = QNFQueryCF CoConst
type QNFQueryAggr = QNFQueryCF CoConst HashBag
type QNFQueryProjF = QNFQueryCF Const
type QNFQueryProj = QNFQueryCF Const HashBag
type QNFQueryCF = QNFQueryDCF EmptyF
type QNFQueryDCF = QNFQuerySPDCF {- sel_f -} HS.HashSet {- prod_f -} HashBag
data QNFQuerySPDCF sel_f prod_f dbg_f col_f f e s =
  QNFQuery
  { qnfColumns :: col_f (QNFProj f e s) (QNFAggr f e s)
    -- ^ What columns kept. Each of these is a column on one of the
    -- QNFProd.
   ,qnfSel :: sel_f (QNFSel e s)
    -- ^ And conjuction of selection. The QNFNames here lack a qnfProd
    -- and qnfSel because they are names of this QNFQuery itself so to
    -- avoid a recursive structure that we need to keep up to date we
    -- just promise that they will be empty.
   ,qnfProd :: prod_f (QNFProd e s)
    -- ^ QNFProd e s refers to multiple rewrites of a single
    -- relation. Essentially {Query (..) (QNFQuery e s)}. Each element
    -- of this set needs to have the same set of QNFQuery's. The binary
    -- operators allowed here MUST expose plans from ONE side. Otherwise
    -- the qnfColumns will be ambiguous. In practice only Product/Join
    -- expose both sides but if there are more in the future know that
    -- qnf machinery will break.
   ,qnfOrigDEBUG' :: dbg_f (Query e s)
   ,qnfHash :: (Int,Int,Int)
    -- ^ Cache the hash. Hashing is done separately for each field so we
    -- don't recompute hashes of unchanged fields after each operation.
  }
  deriving Generic


type QNFConstr (co :: * -> Constraint) sel_f prod_f col_f f e s =
  (co (col_f (QNFProj f e s) (QNFAggr f e s)),
   co (prod_f (QNFProd e s)),
   co (sel_f (QNFSel e s)),
   co e, co s)
instance Hashable (QNFQuerySPDCF sel_f prod_f dbg_f col_f f e s) where
  hashWithSalt s x = hashWithSalt s $ qnfHash x
  {-# INLINE hashWithSalt #-}
  hash = hash . qnfHash
  {-# INLINE hash #-}

type HashableQNF f e s = Hashables4 (QNFProj f e s) (QNFAggr f e s) e s
type HashableQNFSPCF sel_f prod_f col_f f e s =
  (Hashables3
   (col_f (QNFProj f e s) (QNFAggr f e s))
   (prod_f (QNFProd e s))
   (sel_f (QNFSel e s)))
type HashableQNFC c f e s = HashableQNFSPCF HS.HashSet HashBag c f e s
updateHashCol :: HashableQNFSPCF sel_f prod_f col_f f e s =>
                QNFQuerySPDCF sel_f prod_f d col_f f e s
              -> QNFQuerySPDCF sel_f prod_f d col_f f e s
updateHashCol qnf = qnf{
  qnfHash=(hash $ qnfColumns qnf, snd3 $ qnfHash qnf,trd3 $ qnfHash qnf)}
updateHashSel
  :: HashableQNFSPCF sel_f prod_f col_f f e s
  => QNFQuerySPDCF sel_f prod_f d col_f f e s
  -> QNFQuerySPDCF sel_f prod_f d col_f f e s
updateHashSel qnf =
  qnf { qnfHash = (fst3 $ qnfHash qnf,hash $ qnfSel qnf,trd3 $ qnfHash qnf)
      }
updateHashProd
  :: HashableQNFSPCF sel_f prod_f col_f f e s
  => QNFQuerySPDCF sel_f prod_f d col_f f e s
  -> QNFQuerySPDCF sel_f prod_f d col_f f e s
updateHashProd qnf =
  qnf { qnfHash = (hash $ qnfColumns qnf,hash $ qnfSel qnf,hash $ qnfProd qnf)
      }
updateHash
  :: HashableQNFSPCF sel_f prod_f col_f f e s
  => QNFQuerySPDCF sel_f prod_f d col_f f e s
  -> QNFQuerySPDCF sel_f prod_f d col_f f e s
updateHash qnf = qnf{
  qnfHash=(hash $ qnfColumns qnf, hash $ qnfSel qnf,hash $ qnfProd qnf)}
{-# INLINE updateHash #-}
instance (AShow (dbg_f (Query e s)),QNFConstr AShow sel_f prod_f col_f f e s)
  => AShow (QNFQuerySPDCF sel_f prod_f dbg_f col_f f e s)
instance AShow (QNFQueryDCF d c f e s) => Show (QNFQueryDCF d c f e s) where
  show = gshow
instance (QNFConstr Eq sel_f prod_f col_f f e s)
  => Eq (QNFQuerySPDCF sel_f prod_f d col_f f e s) where
  x == y = qnfHash x == qnfHash y
    -- (qnfHash x == qnfHash y
    --  && qnfProd x == qnfProd y
    --  && qnfSel x == qnfSel y
    --  && qnfColumns x == qnfColumns y)
  {-# INLINE (==) #-}

instance (ARead (d (Query e s)),
          QNFConstr ARead sel_f prod_f col_f f e s,
          Hashables2 e s) =>
         ARead (QNFQuerySPDCF sel_f prod_f d col_f f e s)

-- | Like semigroup but concatenates
--
-- qnfColumns :: Either (QNFProj f e s) (QNFAggr f e s)
class QNFSemigroup a where
  colAppend :: a -> a -> Either String a
instance Hashables2 e s => QNFSemigroup (NameMapC c e s) where
  colAppend = return ... (<>)
instance Hashables1 a => QNFSemigroup (HashBag a) where
  colAppend = return ... (<>)
instance (QNFSemigroup a, QNFSemigroup b) =>
         QNFSemigroup (Either a b) where
  colAppend = curry $ \case
    (Left a, Left b) -> fmap Left $ a `colAppend` b
    (Right a, Right b) -> fmap Right $ a `colAppend` b
    _ -> Left "Appending heterogeneous qnfs"
instance (QNFSemigroup a) => QNFSemigroup (Const a b) where
  colAppend a b = fmap Const $ getConst a `colAppend` getConst b
instance (QNFSemigroup b) => QNFSemigroup (CoConst a b) where
  colAppend a b = fmap CoConst $ getCoConst a `colAppend` getCoConst b
instance QNFSemigroup a => QNFSemigroup (Identity a) where
  colAppend a b = fmap Identity $ runIdentity a `colAppend` runIdentity b
instance (Eq e, Eq s, QNFSemigroup (f (Aggr ((QNFName e s))))) =>
         QNFSemigroup (QNFAggr f e s) where
  colAppend (l,e) (r,e') = if e == e'
                           then (,e) <$> colAppend l r
                           else Left "Different aggregations"

-- | The result of toNQNFQuery functions
data NQNFResultDF d f op e s = NQNFResult {
  nqnfResNQNF       :: NQNFQueryDCF d Either f e s,
  nqnfResOrig       :: op,
  -- Map the names exposed by the input qnf to names on the output
  -- qnf. Unless qnfResOp is QProj, QGroup each input name corresponds
  -- to 0 or 1 name in the output. So if qnfResOp is QProj or
  -- qnfResInOutNames has no meaning and will be `const Nothing`
  nqnfResInOutNames :: [((e,QNFCol e s), (e,QNFCol e s))]
  } deriving Generic
instance Hashables3 e s op => Hashable (NQNFResultDF d f op e s)

type NQNFResultI = NQNFResultDF EmptyF HashBag
type NQNFResult = NQNFResultDF Identity HashBag
type NQNFResultF = NQNFResultDF EmptyF

-- Cache
#define MORTAL_QNFS
infixr 5 :->
type k :-> v = HM.HashMap k v
type L e s a = ListT (Either (QNFError e s)) a
data QNFCache e s = QNFCache {
  qnfProduct_cache' :: NameMap e s :-> L e s (NQNFResultI () e s)
  , qnfProduct_cache :: (NQNFQueryI e s,NQNFQueryI e s)
                     :-> L e s (NQNFResultI () e s)
  , qnfSelect_cache :: (Prop (Rel (Expr e)),NQNFQueryI e s)
                     :-> NQNFResultI (Prop (Rel (Expr (QNFName e s, e)))) e s
  , qnfProject_cache :: ([(e,Expr e)],NQNFQueryI e s)
                     :-> NQNFResultI [((QNFName e s,e),Expr (QNFName e s,e))] e s
  , qnfAggregate_cache :: ([(e,Expr (Aggr (Expr e)))],[Expr e],NQNFQueryI e s)
                       :-> NQNFResultI
                       ([((QNFName e s,e),Expr (Aggr (Expr (QNFName e s,e))))],
                        [Expr (QNFName e s,e)])
                       e s
#ifndef MORTAL_QNFS
  , cacheCompact :: Compact ()
  -- ^ we assume that all values of the qnfcache will live forever in
  -- the cache tree. To change this modify this change
  -- MORTAL_QNFS. Defining it will cause the GC to traverse the qnfs
  -- (and them move for GHC < 8.10.1) in each round.
#endif
  } deriving Generic
instance Default (QNFCache e s)
type QNFBuild e s = StateT (QNFCache e s) (Either (QNFError e s))
qnfCached
  :: Hashables1 k
  => (QNFCache e s -> k :-> v)
  -> (QNFCache e s -> k :-> v -> QNFCache e s)
  -> k
  -> Either (QNFError e s) v
  -> QNFBuild e s v
qnfCached get_cache put_cache key mkRet = do
  cache <- get_cache <$> get
  let lrLu = HM.lookup key cache
  case lrLu of
    Just a  -> return a
    Nothing -> do
      ret <- compact' =<< lift mkRet
      modify $
        \cc -> put_cache cc $ HM.insert key ret $ get_cache cc
        -- \cc -> cc{qnfSelect_cache=HM.insert (p,l) ret $ qnfSelect_cache cc}
      return ret
    where
#ifndef MORTAL_QNFS
      compact' x = do
        c <- cacheCompact <$> get
        return $ getCompact $ unsafePerformIO $ compactAdd c x
#else
    compact' = return
#endif
