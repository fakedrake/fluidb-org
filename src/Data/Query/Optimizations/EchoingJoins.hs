{-# LANGUAGE CPP                  #-}
{-# LANGUAGE PatternSynonyms      #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ViewPatterns         #-}
{-# OPTIONS_GHC -Wno-missing-pattern-synonym-signatures #-}
module Data.Query.Optimizations.EchoingJoins
  (joinPermutations,FuzzyQuery) where


import           Control.Monad.Identity
import           Control.Monad.State
import           Control.Monad.Trans.Maybe
import           Control.Utils.Free
import           Data.Foldable
import qualified Data.HashMap.Strict            as HM
import           Data.List
import qualified Data.List.NonEmpty             as NEL
import           Data.Query.Algebra
import           Data.Query.Optimizations.Echo
import           Data.Query.Optimizations.Types
import           Data.Tuple
import           Data.Utils.AShow
import           Data.Utils.Compose
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           GHC.Generics

type L0 = []
type L1 = NEL.NonEmpty
data L2 a = L2 a (L1 a)
  deriving (Functor,Traversable,Foldable)
type QId = Int

type Compose3 f g h = Compose (Compose f g) h
data Cardinality e s
  = Cardinality
  | EqCard [e] (Cardinality e s) (Cardinality e s)
  -- ^ the
  | CardProd (Cardinality e s) (Cardinality e s)
  | CardSel (Cardinality e s)
  | CardEq (Cardinality e s)
  | CardUnion (Cardinality e s) (Cardinality e s)
  | CardGrp (Cardinality e s)
  | CardLim Int
  | CardDrop Int (Cardinality e s)
  | CardQ s
  deriving Generic
instance AShowV2 e s => AShow (Cardinality e s)
instance Default (Cardinality e s) where
  def = Cardinality

-- | Only the joinset needs to be annotated with query ids.
type FuzzyQuery e s = Free (Compose3 ((,) (Cardinality e s)) L1 (TQuery e)) s
wrapFuzzy
  :: EchoSide -> Query e (FuzzyQuery e s) -> FuzzyQuery e s
wrapFuzzy echo q = wrapFuzzy' (getCardinality q) echo q

wrapFuzzy'
  :: Cardinality e s -> EchoSide -> Query e (FuzzyQuery e s) -> FuzzyQuery e s
wrapFuzzy' card e x =
  FreeT
  $ Identity
  $ Free
  $ Compose
  $ Compose
  $ (card,)
  $ return
  $ TQuery { tqQuery = Right x,tqEcho = e }

getCardinality :: Query e (FuzzyQuery e s) -> Cardinality  e s
getCardinality q = case q of
  Q2 o l r -> case o of
    QProd -> CardProd (getCardinality l) (getCardinality r)
    (QJoin _pr) -> CardSel (CardProd (getCardinality l) (getCardinality r))
    (QLeftAntijoin _pr) -> CardSel
      (getCardinality l)
    (QRightAntijoin _pr) -> CardSel
      (getCardinality r)
    QUnion -> CardUnion (getCardinality l) (getCardinality r)
    QProjQuery -> getCardinality r
    QDistinct -> getCardinality r
  Q1 o q -> case o of
    (QSel pr)       -> CardSel (getCardinality q)
    (QGroup x0 exs) -> CardGrp (getCardinality q)
    (QProj x0)      -> getCardinality q
    (QSort exs)     -> getCardinality q
    (QLimit n)      -> CardLim n
    (QDrop n)       -> CardDrop n (getCardinality q)
  Q0 (FuzzyQueryF (card,_)) -> card
  Q0 (FuzzyQueryP s) -> CardQ s

pattern FuzzyQueryF :: f (g (h (FreeT (Compose3 f g h) Identity a)))
                    -> FreeT (Compose3 f g h) Identity a
pattern FuzzyQueryF a = FreeT (Identity (Free (Compose (Compose a))))
pattern FuzzyQueryP :: a -> FreeT f Identity a
pattern FuzzyQueryP s = FreeT (Identity (Pure s))

-- XXX: Create the echo area
setEcho :: EchoSide -> FuzzyQuery e s -> FuzzyQuery e s
setEcho NoEcho fq = fq
setEcho echo  fq  = wrapFuzzy echo $ Q0 fq

-- | Prioritize the left hand side cardinality
combFuzz :: FuzzyQuery e s -> FuzzyQuery e s -> FuzzyQuery e s
combFuzz (FuzzyQueryF (card,qs)) (FuzzyQueryF (_card',qs')) =
  FuzzyQueryF (card,qs <> qs')
combFuzz (FuzzyQueryF (card,qs)) (FuzzyQueryP s) =
  FuzzyQueryF (card,qs <> pure (tunnelQuery $ Q0 $ return s))
combFuzz (FuzzyQueryP s) (FuzzyQueryF (card,qs)) =
  FuzzyQueryF (card,pure (tunnelQuery $ Q0 $ return s) <> qs)
combFuzz (FuzzyQueryP s) (FuzzyQueryP s') =
  FuzzyQueryF
    (CardQ s
    ,pure (tunnelQuery $ Q0 $ return s)
     <> pure (tunnelQuery $ Q0 $ return s'))
combFuzz _ _ = error "Unreachable!"

data JoinSet e s =
  JoinSet
  { jsProps :: L0 (JProp e)
   ,jsQs    :: L1 (QId,Query e (Either s (JoinSet e s))) --
   ,jsUid   :: Hashables2 e s => Int
  }

instance (AShow e,AShowV s) => AShow (JoinSet e s) where
  ashow' JoinSet {..} =
    recSexp "JoinSet" [("jsProps",ashow' jsProps),("jsQs",ashow' jsQs)]

--
-- aprint $ queryToJoinSet (==) $ Q1 (QProj []) $ (J (P0 (R2 REq (R0 (E0 'a')) (R0 (E0 'b')) )) (Q0 'a') (Q0 'b'))
queryToJoinSet
  :: forall e0 e s .
  Hashables2 e s
  => SymEmbedding e0 s e
  -> Query e s
  -> JoinSet e s
queryToJoinSet emb = go
  where
    go :: Query e s -> JoinSet e s
    go = recur [] [] []
    recur ps prims todo = \case
      S p q -> recur (ps ++ toList (propQnfAnd p)) prims todo q
      J p q1 q2 -> recur (ps ++ toList (propQnfAnd p)) prims (q2 : todo) q1
      Q2 QProd q1 q2 -> recur ps prims (q2 : todo) q1
      Q2 o l r -> fin
        ps
        (Q2 o (Q0 $ Right $ go l) (Q0 $ Right $ go r) NEL.:| prims)
        todo
      Q1 o q -> fin ps (Q1 o (Q0 $ Right $ go q) NEL.:| prims) todo
      Q0 s -> fin ps (Q0 (Left s) NEL.:| prims) todo
    fin ps prims (q:todo) = recur ps (toList prims) todo q
    fin ps prims [] = mkJoinSet (fmap4 annotateE ps) prims'
      where
        canPushE e query = embedIsLit emb e || refersToQ query e
        refersToQ :: Query e (Either s (JoinSet e s)) -> e -> Bool
        refersToQ q e = (`any` querySchemaNaive q) $ \case
          Left s -> either (embedInS emb e) ((`refersToQ` e) . jsToQ) s
          Right (Left p,_) -> elem' e $ fst <$> p
          Right (Right (p,_),_) -> elem' e $ fst <$> p
          where
            jsToQ = foldl1 (Q2 QProd) . fmap snd . jsQs
            elem' :: e -> [e] -> Bool
            elem' = any . symEq emb
        annotateE e = (fst <$> find (canPushE e . snd) prims',e)
        prims' = NEL.zip (0 NEL.:| [1 ..]) prims

instance Hashables2 e s => Eq (JoinSet e s) where
  a == b = hash a == hash b
instance Hashables2 e s => Hashable (JoinSet e s) where
  hashWithSalt salt v = hashWithSalt salt (jsUid v)

mkJoinSet
  :: L0 (JProp e) -> L1 (QId,Query e (Either s (JoinSet e s))) -> JoinSet e s
mkJoinSet props qs =
  JoinSet
  { jsProps = props
   ,jsQs = qs
   ,jsUid = hash qs
  }

type JProp e = Prop (Rel (Expr (Maybe QId,e)))

-- | Props separated according to left and right.
data SepProps e =
  SepProps { spLeftNonEq,spRightNonEq,spLeftEq,spRightEq,spConnEq,spConnNonEq
               :: [JProp e]
           }
  deriving Generic
instance AShow e => AShow (SepProps e)

mkSepProps :: L1 QId -> L1 QId -> [JProp e] -> SepProps e
mkSepProps lIds rIds ps =
  let (eqP_,nonEqP_) = partition isEquality ps
      (spRightEq,eqP__) = partition (isPushable rIds) eqP_
      (spLeftEq,spConnEq) = partition (isPushable lIds) eqP__
      (spLeftNonEq,nonEqP__) = partition (isPushable lIds) nonEqP_
      (spRightNonEq,spConnNonEq) = partition (isPushable rIds) nonEqP__
  in SepProps { .. }

pattern JoinSetSingleQuery qid q <-
  JoinSet { jsProps = [],jsQs = (qid,q) NEL.:| [], jsUid=_ }
pattern JoinSetSelectSingle p ps qid q <-
  JoinSet { jsProps = p:ps,jsQs = (qid,q) NEL.:| [] }
pattern JoinSetProduct qid q qs <-
  JoinSet { jsProps = [],jsQs = (qid,q) NEL.:| qs }
pattern JoinSetAtLeast2 ps q0 q1 qs <-
  JoinSet { jsProps = ps,jsQs = q0 NEL.:| (q1 : qs),jsUid = _}

data PJState e s =
  PJState { pjsEchoId :: EchoId
          ,pjsCache   :: HM.HashMap (JoinSet e s) (FuzzyQuery e s)
          } deriving Generic
instance Default (PJState e s)


selQ :: EchoSide -> [JProp e] -> FuzzyQuery e s -> FuzzyQuery e s
selQ echo p q =
  if null p then setEcho echo q
  else wrapFuzzy echo $ S (fmap3 snd $ foldl1' And p) $ Q0 q

eqJoinQ
  :: EchoSide
  -> [JProp e] -- Must  be equalities.
  -> FuzzyQuery e s
  -> FuzzyQuery e s
  -> Maybe (FuzzyQuery e s)
eqJoinQ _echo [] _q1 _q2 = Nothing
eqJoinQ echo p q1 q2 =
  Just
  $ wrapFuzzy' (eqJoinCard (toList5 p) q1 q2) echo
  $ J (fmap3 snd $ foldl1' And p) (Q0 q1) (Q0 q2)


-- Partitiion toi the following categories:
--
-- * conn : Props between l,r -- can't push use as join
-- * eqj : Equijoins not connecting
-- * pl : Pushable to left
-- * pr : Pushable to right
--
-- The rules for generating are the following:
--
-- * We always push eqj
-- * We never pish conn
-- * We non-deterministically push or don't push the [pl ++ pr]

eqJoinCard
  :: [e] -> FuzzyQuery e s -> FuzzyQuery e s -> Cardinality e s
eqJoinCard a l r = EqCard a (getCardinality $ Q0 l) (getCardinality $ Q0 r)

isPushable :: L1 QId -> Prop (Rel (Expr (Maybe QId, e))) -> Bool
isPushable as (toList5 . fmap3 swap -> p) =
  all (`elem` as) p

isEquality :: Prop (Rel (Expr e')) -> Bool
isEquality (P0 (R2 REq (R0 (E0 _)) (R0 (E0 _)))) = True
isEquality _                                     = False

onEitherSide :: L0 (L1 a,L1 a) -> a -> L0 (L1 a,L1 a)
onEitherSide res q = do
  (ls,rs) <- res
  [(pure q <> ls,rs),(ls,pure q <> rs)]

cachedJoin
  :: Hashables2 e s
  => (EchoId -> JoinSet e s -> MaybeT (State (PJState e s)) (FuzzyQuery e s))
  -> JoinSet e s
  -> MaybeT (State (PJState e s)) (FuzzyQuery e s)
cachedJoin f js = do
  gets (HM.lookup js . pjsCache) >>= \case
    Just x -> return x
    Nothing -> do
      tid <- gets pjsEchoId
      modify $ \pjs -> pjs { pjsEchoId = pjsEchoId pjs + 1 }
      -- Don't worry that we are not reserving the return value on the
      -- cache while evaluating.
      ret <- f tid js
      modify $ \pjs -> pjs { pjsCache = HM.insert js ret $ pjsCache pjs }
      return ret


-- | From all the possible separations of the the joinset prune the
-- ones that are invalid (ie contain products) and create fuzzy
-- queries with the rest.
--
-- What is going on with
echoingJoins
  :: Monad m
  => (JoinSet e s -> MaybeT m (FuzzyQuery e s))
  -> EchoId
  -> JoinSet e s
  -> MaybeT m (FuzzyQuery e s)
echoingJoins recur echoId = \case
  JoinSetSingleQuery _qid q -> onlyWrap q
  JoinSetSelectSingle p ps _qid q -> onlyWrap
    $ S (fmap3 snd $ foldl' And p ps) q
  JoinSetProduct {} -> mzero
  JoinSetAtLeast2 ps l0 r0 qs -> commitFuzz $ do
    (ls,rs) <- mkListT $ return $ foldl' onEitherSide [(pure l0,pure r0)] qs
    let SepProps {..} = mkSepProps (fst <$> ls) (fst <$> rs) ps
    -- Build the tunnels.
    let allNonEq = spLeftNonEq <> spRightNonEq <> spConnNonEq
        spLeftAll = spLeftEq <> spLeftNonEq
        spRightAll = spRightEq <> spRightNonEq
    -- Push down as few props as possible to make general purpose IR.
    eqJoinLeft <- liftL $ recur $ mkJoinSet spLeftEq ls
    eqJoinRight <- liftL $ recur $ mkJoinSet spRightEq rs
    -- Push down as many props as possible to make optimal IR.
    optimalLeft <- liftL $ recur $ mkJoinSet spLeftAll ls
    optimalRight <- liftL $ recur $ mkJoinSet spRightAll rs
    -- XXX: we have S noEq (J eq optL optR) but not J (eq /\ noEq) optL optR.
    let fullEqJoinM = eqJoinQ (TExit echoId) spConnEq eqJoinLeft eqJoinRight
        optimalJoinM = eqJoinQ (TEntry echoId) spConnEq optimalLeft optimalRight
    case (fullEqJoinM,optimalJoinM) of
      (Nothing,Nothing) -> mzero
      (Just fullEqJoin,Just optimalJoin) -> return
        $ selQ (TEntry echoId) allNonEq fullEqJoin
        `combFuzz` selQ (TEntry echoId) spConnNonEq optimalJoin
      _ -> error
        "eqJoinQ should return Just or Nothing only based on the value of mustJoin"
  _ -> error "unreachable"
  where
    liftL :: Monad m => MaybeT m a -> ListT m a
    liftL m = lift (runMaybeT m) >>= maybe mzero return
    onlyWrap q =
      wrapFuzzy NoEcho . fmap (either return id) <$> traverse2 recur q


-- | Transform a ListT of FuzzQueries into a single FuzzQuery that is
-- the combination of all fuzzes. If the ListT monad is empty then the
-- output monad is also empty.
commitFuzz :: Monad m => ListT m (FuzzyQuery e s) -> MaybeT m (FuzzyQuery e s)
commitFuzz m = lift (runListT m) >>= \case
  [] -> mzero
  xs -> return $ foldl1 combFuzz xs


joinPermutations
  :: Hashables2 e s
  => SymEmbedding e0 s e
  -> Query e s
  -> Maybe (FuzzyQuery e s)
joinPermutations emb q =
  (`evalState` def)
  $ runMaybeT
  $ cachedJoin (echoingJoins $ fix $ \f -> echoingJoins f def)
  $ queryToJoinSet emb q

-- | Remove the noise and tunnels from FuzzQuery and just show that.
ashowFuzz :: forall e s . (AShowV2 e s) => FuzzyQuery e s -> SExp
ashowFuzz (FreeT (Identity (Pure x))) = ashow' x
ashowFuzz (FreeT (Identity (Free (Compose (Compose (card,m)))))) =
  ashow' (card,ashowTQ . fmap ashowFuzz <$> toList m)
  where
    ashowTQ :: TQuery e SExp -> SExp
    ashowTQ = either ashow' ashow' . tqQuery

-- SHould be Just..
testJe =
  ashowFuzz
  <$> joinPermutations
    eqSymEmbedding
    (mkQ
       [("lo_orderdate","d_datekey")
       ,("lo_partkey","p_partkey")
       ,("lo_suppkey","s_suppkey")])


mkQ x = S p $ prod p where p = mkP x
mkP = foldl1' And . fmap (\(l,r) -> P0 (R2 REq (R0 (E0 l)) (R0 (E0 r))))
prod :: Prop (Rel (Expr String)) -> Query String [String]
prod = foldl1' (Q2 QProd) . fmap Q0 . groupBy commonPrefix . sort . toList3
  where
    commonPrefix [] []           = True
    commonPrefix ('_':_) ('_':_) = True
    commonPrefix (l:ls) (r:rs)   = if l == r then commonPrefix ls rs else False
    commonPrefix _ _             = False

q1 =
  (((S
       (((P2
            PAnd
            (P2
               PAnd
               (P0 (R2 REq (R0 (E0 ("lo_orderdate"))) (R0 (E0 ("d_datekey")))))
               (P0 (R2 REq (R0 (E0 ("lo_partkey"))) (R0 (E0 ("p_partkey"))))))
            (P0 (R2 REq (R0 (E0 ("lo_suppkey"))) (R0 (E0 ("s_suppkey"))))))))
       (Q2
          QProd
          (Q2
             QProd
             (Q2
                QProd
                (Q0 ["lo_orderdate","lo_partkey","lo_suppkey","lo_revenue"])
                (Q0 ["d_datekey","d_year"]))
             (Q0 ["p_category","p_partkey","p_brand1"]))
          (Q0 ["s_region","s_suppkey"])))))

q0 =
  (Q1
     (QSort [E0 ("d_year"),E0 ("p_brand1")])
     (Q1
        (QGroup
           [("tmpSym0",E0 (NAggr AggrSum (E0 ("lo_revenue"))))
           ,("d_year",E0 (NAggr AggrFirst (E0 ("d_year"))))
           ,("p_brand1",E0 (NAggr AggrFirst (E0 ("p_brand1"))))]
           [E0 ("d_year"),E0 ("p_brand1")])
        (S
           (P2
              PAnd
              (P2
                 PAnd
                 (P2
                    PAnd
                    (P2
                       PAnd
                       (P0
                          (R2
                             REq
                             (R0 (E0 ("lo_orderdate")))
                             (R0 (E0 ("d_datekey")))))
                       (P0
                          (R2
                             REq
                             (R0 (E0 ("lo_partkey")))
                             (R0 (E0 ("p_partkey"))))))
                    (P0
                       (R2 REq (R0 (E0 ("lo_suppkey"))) (R0 (E0 ("s_suppkey"))))))
                 (P0 (R2 REq (R0 (E0 ("p_category"))) (R0 (E0 ("p_category"))))))
              (P0 (R2 REq (R0 (E0 ("s_region"))) (R0 (E0 ("s_region"))))))
           (Q2
              QProd
              (Q2
                 QProd
                 (Q2
                    QProd
                    (Q0 ["lo_orderdate","lo_partkey","lo_suppkey","lo_revenue"])
                    (Q0 ["d_datekey","d_year"]))
                 (Q0 ["p_category","p_partkey","p_brand1"]))
              (Q0 ["s_region","s_suppkey"])))))
