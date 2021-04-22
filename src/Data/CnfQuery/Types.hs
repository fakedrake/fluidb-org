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

module Data.CnfQuery.Types
  ( CNFError(..)
  , CNFCache(..)
  , CNFBuild
  , CNFSelName
  , CNFNameSPC(..)
  , CNFColSPC
  , CNFNameC
  , CNFQueryDCF
  , CNFQuerySPDCF(..)
  , CNFQueryCF
  , CNFName
  , CNFNameAggr
  , CNFNameProj
  , CNFQueryAggrF
  , CNFQueryProjF
  , CNFProd
  , CNFSel
  , CNFProj
  , CNFAggr
  , NameMap
  , NameMapC
  , NameMapAggr
  , NameMapProj
  , NCNFQuery
  , NCNFQueryI
  , NCNFQueryCF
  , NCNFQueryDCF
  , NCNFQueryF
  , NCNFQueryAggrF
  , NCNFQueryProjF
  , CNFQueryAggr
  , CNFQueryProj
  , CNFQuery
  , CNFQueryI
  , CNFQueryF
  , CNFCol
  , CNFColC
  , CNFColProj
  , CNFColAggr
  , CNFConstr
  , CNFSemigroup(..)
  , NCNFResultDF(..)
  , NCNFResultF
  , NCNFResultI
  , NCNFResult
  , HashableCNF
  , HashableCNFSPCF
  , HashableCNFC
  , updateHash
  , updateHashProd
  , updateHashSel
  , updateHashCol
  , cnfCached
  ) where

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
import           Data.CnfQuery.HashBag
import           Data.Utils.Default
import           Data.Utils.EmptyF
import           Data.Utils.Const
import           Data.Utils.Hashable
import           GHC.Generics


data CNFError e s = CNFErrorMsg (AShowStr e s)
  | CNFProjNameMapOverAggrCnf
  | CNFUnknownSymbol e [e]
  | CNFSchemaMismatch [e] [e]
  | CNFMissingSchema (Query e s) s
  deriving (Eq, Generic)
instance AShowError e s (CNFError e s)
instance (AShowV e, AShowV s) => AShow (CNFError e s)
data CNFNameSPC sel_f prod_f col_f e s
  = PrimaryCol e s Int
  | Column (CNFColSPC sel_f prod_f col_f e s) Int
    -- ^ a a column c that refers to a:
    --
    -- CNF[P[c],S[c],Prod[Query[a ~ CNF]]]
  | NonSymbolName e
     -- ^ A literal.
  deriving Generic
type CNFName = CNFNameC Either
type CNFNameAggr = CNFNameC CoConst
type CNFNameProj = CNFNameC  Const
type CNFNameC = CNFNameSPC HS.HashSet HashBag
instance Default e => Default (CNFNameSPC sel_f prod_f col_f e s) where
  def = NonSymbolName def
instance CNFConstr AShow sel_f prod_f col_f Identity e s =>
  AShow (CNFNameSPC sel_f prod_f col_f e s)
  where
    ashow' = \case
      PrimaryCol e s i -> sexp "PrimaryCol" [ashow' e,ashow' s,ashow' i]
      Column c i -> sexp "Column" [ashow' $ hash c,ashow' i]
      NonSymbolName e -> sexp "NonSymbolName" [ashow' e]

instance (Hashables2 e s, CNFConstr ARead sel_f prod_f col_f Identity e s) => ARead (CNFNameSPC sel_f prod_f col_f e s)
instance (CNFConstr Eq sel_f prod_f col_f Identity e s,
          CNFConstr Hashable sel_f prod_f col_f Identity e s) =>
         Hashable (CNFNameSPC sel_f prod_f col_f e s)
instance CNFConstr AShow sel_f prod_f col_f Identity e s =>
         Show (CNFNameSPC sel_f prod_f col_f e s) where
  show = gshow
instance CNFConstr Eq sel_f prod_f col_f Identity e s =>
         Eq (CNFNameSPC sel_f prod_f col_f e s) where
  (==) = curry $ \case
    (PrimaryCol e s i, PrimaryCol e' s' i') -> e == e' && s == s' && i == i'
    (Column c i, Column c' i') -> c == c' && i == i'
    (NonSymbolName e, NonSymbolName e') -> e == e'
    _ -> False

-- | All the information required to match a CNFName to a
-- corresponding table.
type NameMapC c e s = HM.HashMap e (CNFColC c e s)
type NameMapAggr e s = NameMapC CoConst e s
type NameMapProj e s = NameMapC Const e s
type NameMap e s = NameMapC Either e s
type NCNFQueryDCF d c f e s = (NameMapC c e s, CNFQueryDCF d c f e s)
type NCNFQueryCF c f e s = NCNFQueryDCF EmptyF c f e s
type NCNFQuery e s = NCNFQueryDCF Identity Either HashBag e s
-- A cnf query without the cnfQueryDebug
type NCNFQueryI e s = NCNFQueryDCF EmptyF Either HashBag e s
type NCNFQueryF f e s = NCNFQueryCF Either f e s
type NCNFQueryProjF f e s = NCNFQueryCF Const f e s
type NCNFQueryAggrF f e s = NCNFQueryCF CoConst f e s
type CNFColSPC s p c = CNFQuerySPDCF s p EmptyF c Identity
type CNFColC (c :: * -> * -> *) = (CNFQueryCF c Identity :: * -> * -> *)
type CNFCol = CNFColC Either
type CNFColProj = CNFColC Const
type CNFColAggr = CNFColC CoConst
-- Invariant: The column on Right refers to an E0, remember that
-- selections are under the aggregations and therefore can't be
_cnfSelNameSane :: CNFSelName e s -> Bool
_cnfSelNameSane = either (const True) $ projSane . cnfColumns where
  projSane = \case
    Const (Identity (E0 _)) -> True
    _ -> False
type CNFSelName e s = Either e (CNFQuerySPDCF EmptyF EmptyF EmptyF Const Identity e s)
type CNFProd e s = HS.HashSet (Query (CNFName e s) (Either s (CNFQueryI e s)))
type CNFSel e s =  Prop (Rel (Expr (CNFSelName e s)))
type CNFProj f e s = f (Expr (CNFName e s))
type CNFAggr f e s = (f (Aggr (Expr (CNFName e s))), HS.HashSet (Expr (CNFName e s)))
type CNFQueryI = CNFQueryF HashBag
type CNFQuery = CNFQueryDCF Identity Either HashBag
type CNFQueryF = CNFQueryCF Either
type CNFQueryAggrF = CNFQueryCF CoConst
type CNFQueryAggr = CNFQueryCF CoConst HashBag
type CNFQueryProjF = CNFQueryCF Const
type CNFQueryProj = CNFQueryCF Const HashBag
type CNFQueryCF = CNFQueryDCF EmptyF
type CNFQueryDCF = CNFQuerySPDCF HS.HashSet HashBag
data CNFQuerySPDCF sel_f prod_f dbg_f col_f f e s = CNFQuery {
  cnfColumns    :: col_f (CNFProj f e s) (CNFAggr f e s),
  -- ^ What columns kept. Each of these is a column on one of the
  -- CNFProd.
  cnfSel        :: sel_f (CNFSel e s),
  -- ^ And conjuction of selection. The CNFNames here lack a cnfProd
  -- and cnfSel because they are names of this CNFQuery itself so to
  -- avoid a recursive structure that we need to keep up to date we
  -- just promise that they will be empty.
  cnfProd       :: prod_f (CNFProd e s),
  -- ^ CNFProd e s refers to multiple rewrites of a single
  -- relation. Essentially {Query (..) (CNFQuery e s)}. Each element
  -- of this set needs to have the same set of CNFQuery's. The binary
  -- operators allowed here MUST expose plans from ONE side. Otherwise
  -- the cnfColumns will be ambiguous. In practice only Product/Join
  -- expose both sides but if there are more in the future know that
  -- cnf machinery will break.
  cnfOrigDEBUG' :: dbg_f (Query e s),
  cnfHash       :: (Int,Int,Int)
  -- ^ Cache the hash. Hashing is done separately for each field so we
  -- don't recompute hashes of unchanged fields after each operation.
  } deriving Generic
type CNFConstr (co :: * -> Constraint) sel_f prod_f col_f f e s =
  (co (col_f (CNFProj f e s) (CNFAggr f e s)),
   co (prod_f (CNFProd e s)),
   co (sel_f (CNFSel e s)),
   co e, co s)
instance Hashable (CNFQuerySPDCF sel_f prod_f dbg_f col_f f e s) where
  hashWithSalt s x = hashWithSalt s $ cnfHash x
  {-# INLINE hashWithSalt #-}
  hash = hash . cnfHash
  {-# INLINE hash #-}

type HashableCNF f e s = Hashables4 (CNFProj f e s) (CNFAggr f e s) e s
type HashableCNFSPCF sel_f prod_f col_f f e s =
  (Hashables3
   (col_f (CNFProj f e s) (CNFAggr f e s))
   (prod_f (CNFProd e s))
   (sel_f (CNFSel e s)))
type HashableCNFC c f e s = HashableCNFSPCF HS.HashSet HashBag c f e s
updateHashCol :: HashableCNFSPCF sel_f prod_f col_f f e s =>
                CNFQuerySPDCF sel_f prod_f d col_f f e s
              -> CNFQuerySPDCF sel_f prod_f d col_f f e s
updateHashCol cnf = cnf{
  cnfHash=(hash $ cnfColumns cnf, snd3 $ cnfHash cnf,trd3 $ cnfHash cnf)}
updateHashSel
  :: HashableCNFSPCF sel_f prod_f col_f f e s
  => CNFQuerySPDCF sel_f prod_f d col_f f e s
  -> CNFQuerySPDCF sel_f prod_f d col_f f e s
updateHashSel cnf =
  cnf { cnfHash = (fst3 $ cnfHash cnf,hash $ cnfSel cnf,trd3 $ cnfHash cnf)
      }
updateHashProd
  :: HashableCNFSPCF sel_f prod_f col_f f e s
  => CNFQuerySPDCF sel_f prod_f d col_f f e s
  -> CNFQuerySPDCF sel_f prod_f d col_f f e s
updateHashProd cnf =
  cnf { cnfHash = (hash $ cnfColumns cnf,hash $ cnfSel cnf,hash $ cnfProd cnf)
      }
updateHash
  :: HashableCNFSPCF sel_f prod_f col_f f e s
  => CNFQuerySPDCF sel_f prod_f d col_f f e s
  -> CNFQuerySPDCF sel_f prod_f d col_f f e s
updateHash cnf = cnf{
  cnfHash=(hash $ cnfColumns cnf, hash $ cnfSel cnf,hash $ cnfProd cnf)}
{-# INLINE updateHash #-}
instance (AShow (dbg_f (Query e s)),CNFConstr AShow sel_f prod_f col_f f e s)
  => AShow (CNFQuerySPDCF sel_f prod_f dbg_f col_f f e s)
instance AShow (CNFQueryDCF d c f e s) => Show (CNFQueryDCF d c f e s) where
  show = gshow
instance (CNFConstr Eq sel_f prod_f col_f f e s)
  => Eq (CNFQuerySPDCF sel_f prod_f d col_f f e s) where
  x == y = cnfHash x == cnfHash y
    -- (cnfHash x == cnfHash y
    --  && cnfProd x == cnfProd y
    --  && cnfSel x == cnfSel y
    --  && cnfColumns x == cnfColumns y)
  {-# INLINE (==) #-}

instance (ARead (d (Query e s)),
          CNFConstr ARead sel_f prod_f col_f f e s,
          Hashables2 e s) =>
         ARead (CNFQuerySPDCF sel_f prod_f d col_f f e s)

-- | Like semigroup but concatenates
--
-- cnfColumns :: Either (CNFProj f e s) (CNFAggr f e s)
class CNFSemigroup a where
  colAppend :: a -> a -> Either String a
instance Hashables2 e s => CNFSemigroup (NameMapC c e s) where
  colAppend = return ... (<>)
instance Hashables1 a => CNFSemigroup (HashBag a) where
  colAppend = return ... (<>)
instance (CNFSemigroup a, CNFSemigroup b) =>
         CNFSemigroup (Either a b) where
  colAppend = curry $ \case
    (Left a, Left b) -> fmap Left $ a `colAppend` b
    (Right a, Right b) -> fmap Right $ a `colAppend` b
    _ -> Left "Appending heterogeneous cnfs"
instance (CNFSemigroup a) => CNFSemigroup (Const a b) where
  colAppend a b = fmap Const $ getConst a `colAppend` getConst b
instance (CNFSemigroup b) => CNFSemigroup (CoConst a b) where
  colAppend a b = fmap CoConst $ getCoConst a `colAppend` getCoConst b
instance CNFSemigroup a => CNFSemigroup (Identity a) where
  colAppend a b = fmap Identity $ runIdentity a `colAppend` runIdentity b
instance (Eq e, Eq s, CNFSemigroup (f (Aggr (Expr (CNFName e s))))) =>
         CNFSemigroup (CNFAggr f e s) where
  colAppend (l,e) (r,e') = if e == e'
                           then (,e) <$> colAppend l r
                           else Left "Different aggregations"

-- | The result of toNCNFQuery functions
data NCNFResultDF d f op e s = NCNFResult {
  ncnfResNCNF       :: NCNFQueryDCF d Either f e s,
  ncnfResOrig       :: op,
  -- Map the names exposed by the input cnf to names on the output
  -- cnf. Unless cnfResOp is QProj, QGroup each input name corresponds
  -- to 0 or 1 name in the output. So if cnfResOp is QProj or
  -- cnfResInOutNames has no meaning and will be `const Nothing`
  ncnfResInOutNames :: [((e,CNFCol e s), (e,CNFCol e s))]
  }

type NCNFResultI = NCNFResultDF EmptyF HashBag
type NCNFResult = NCNFResultDF Identity HashBag
type NCNFResultF = NCNFResultDF EmptyF

-- Cache
#define MORTAL_CNFS
infixr 5 :->
type k :-> v = HM.HashMap k v
type L e s a = ListT (Either (CNFError e s)) a
data CNFCache e s = CNFCache {
  cnfProduct_cache' :: NameMap e s :-> L e s (NCNFResultI () e s)
  , cnfProduct_cache :: (NCNFQueryI e s,NCNFQueryI e s)
                     :-> L e s (NCNFResultI () e s)
  , cnfSelect_cache :: (Prop (Rel (Expr e)),NCNFQueryI e s)
                     :-> NCNFResultI (Prop (Rel (Expr (CNFName e s, e)))) e s
  , cnfProject_cache :: ([(e,Expr e)],NCNFQueryI e s)
                     :-> NCNFResultI [((CNFName e s,e),Expr (CNFName e s,e))] e s
  , cnfAggregate_cache :: ([(e,Expr (Aggr (Expr e)))],[Expr e],NCNFQueryI e s)
                       :-> NCNFResultI
                       ([((CNFName e s,e),Expr (Aggr (Expr (CNFName e s,e))))],
                        [Expr (CNFName e s,e)])
                       e s
#ifndef MORTAL_CNFS
  , cacheCompact :: Compact ()
  -- ^ we assume that all values of the cnfcache will live forever in
  -- the cache tree. To change this modify this change
  -- MORTAL_CNFS. Defining it will cause the GC to traverse the cnfs
  -- (and them move for GHC < 8.10.1) in each round.
#endif
  } deriving Generic
instance Default (CNFCache e s)
type CNFBuild e s = StateT (CNFCache e s) (Either (CNFError e s))
cnfCached :: Hashables1 k =>
            (CNFCache e s -> k :-> v)
          -> (CNFCache e s -> k :-> v -> CNFCache e s)
          -> k
          -> Either (CNFError e s) v
          -> CNFBuild e s v
cnfCached get_cache put_cache key mkRet = do
  cache <- get_cache <$> get
  let lrLu = HM.lookup key cache
  case lrLu of
    Just a  -> return a
    Nothing -> do
      ret <- compact' =<< lift mkRet
      modify $
        \cc -> put_cache cc $ HM.insert key ret $ get_cache cc
        -- \cc -> cc{cnfSelect_cache=HM.insert (p,l) ret $ cnfSelect_cache cc}
      return ret
    where
#ifndef MORTAL_CNFS
      compact' x = do
        c <- cacheCompact <$> get
        return $ getCompact $ unsafePerformIO $ compactAdd c x
#else
    compact' = return
#endif
