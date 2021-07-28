{-# LANGUAGE CPP                  #-}
{-# LANGUAGE PatternSynonyms      #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ViewPatterns         #-}
module Data.Query.Optimizations.EchoingJoins
  (joinPermutations) where


import           Control.Monad.Identity
import           Control.Monad.State
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


-- | Only the joinset needs to be annotated with query ids.
type FuzzyQuery e s = Free (Compose L1 (TQuery e)) s
wrapFuzzy :: EchoSide -> Query e (FuzzyQuery e s) -> FuzzyQuery e s
wrapFuzzy e x =
  FreeT
  $ Identity
  $ Free
  $ Compose
  $ return
  $ TQuery { tqQuery = Right x,tqEcho = e }

pattern FuzzyQueryF :: f (g (FreeT (Compose f g) Identity a))
                    -> FreeT (Compose f g) Identity a
pattern FuzzyQueryF a = FreeT (Identity (Free (Compose a)))
pattern FuzzyQueryP :: a -> FreeT f Identity a
pattern FuzzyQueryP s = FreeT (Identity (Pure s))

combFuzz :: FuzzyQuery e s -> FuzzyQuery e s -> FuzzyQuery e s
combFuzz (FuzzyQueryF qs) (FuzzyQueryF qs') = FuzzyQueryF $ qs <> qs'
combFuzz (FuzzyQueryF qs) (FuzzyQueryP s) =
  FuzzyQueryF $ qs <> pure (tunnelQuery $ Q0 $ return s)
combFuzz (FuzzyQueryP s) (FuzzyQueryF qs) =
  FuzzyQueryF $ pure (tunnelQuery $ Q0 $ return s) <> qs
combFuzz (FuzzyQueryP s) (FuzzyQueryP s') =
  FuzzyQueryF
  $ pure (tunnelQuery $ Q0 $ return s)
  <> pure (tunnelQuery $ Q0 $ return s')
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
  SepProps
  { spLeftNonEq,spRightNonEq,spLeftEq,spRightEq,spConnEq,spConnNonEq
      :: [JProp e]
  }

mkSepProps :: L1 QId -> L1 QId -> [JProp e] -> SepProps e
mkSepProps lIds rIds ps =
  let (eqP_,nonEqP_) = partition isEquality ps
      (spRightEq,eqP__) = partition (isPushable lIds) eqP_
      (spLeftEq,spConnEq) = partition (isPushable rIds) eqP__
      (spLeftNonEq,nonEqP__) = partition (isPushable lIds) nonEqP_
      (spRightNonEq,spConnNonEq) = partition (isPushable rIds) nonEqP__
  in SepProps { .. }

pattern JoinSetSingleQuery qid q <-
  JoinSet { jsProps = [],jsQs = (qid,q) NEL.:| [], jsUid=_ }
pattern JoinSetSelectSingle p ps qid q =
  JoinSet { jsProps = p:ps,jsQs = (qid,q) NEL.:| [] }
pattern JoinSetProduct qid q qs =
  JoinSet { jsProps = [],jsQs = (qid,q) NEL.:| qs }
pattern JoinSetAtLeast2 ps q0 q1 qs =
  JoinSet { jsProps = ps,jsQs = q0 NEL.:| (q1 : qs) }

data PJState e s =
  PJState { pjsEchoId :: EchoId
          ,pjsCache   :: HM.HashMap (JoinSet e s) (FuzzyQuery e s)
          } deriving Generic
instance Default (PJState e s)


selQ :: EchoSide -> [JProp e] -> FuzzyQuery e s -> FuzzyQuery e s
selQ echo p q =
  if null p then q else wrapFuzzy echo $ S (fmap3 snd $ foldl1' And p) $ Q0 q

joinQ :: EchoSide
      -> [JProp e]
      -> FuzzyQuery e s
      -> FuzzyQuery e s
      -> FuzzyQuery e s
joinQ echo p q1 q2 =
  wrapFuzzy echo $ J (fmap3 snd $ foldl1' And p) (Q0 q1) (Q0 q2)


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

isPushable :: L1 QId -> Prop (Rel (Expr (Maybe QId, e))) -> Bool
isPushable as (toList5 . fmap3 swap -> p) =
  all (`elem` p) as

isEquality :: Prop (Rel (Expr e')) -> Bool
isEquality (P0 (R2 REq (R0 (E0 _)) (R0 (E0 _)))) = True
isEquality _                                     = False

--
-- Finally we create echo tunnels between each of the resulting
-- queries and the pure equijoin query. This is a heuristic that
-- assumes that the particular query is useful to be considered when
-- considering andy of the queries in question.

onEitherSide :: L0 (L1 a,L1 a) -> a -> L0 (L1 a,L1 a)
onEitherSide res q = do
  (ls,rs) <- res
  [(pure q <> ls,rs),(ls,pure q <> rs)]
cachedJoin
  :: (Monad m,Hashables2 e s)
  => (EchoId -> JoinSet e s -> StateT (PJState e s) m (FuzzyQuery e s))
  -> JoinSet e s
  -> StateT (PJState e s) m (FuzzyQuery e s)
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

echoingJoins
  :: MonadPlus m
  => (JoinSet e s -> m (FuzzyQuery e s))
  -> EchoId
  -> JoinSet e s
  -> m (FuzzyQuery e s)
echoingJoins recur echoId = \case
  JoinSetSingleQuery _qid q -> onlyWrap q
  JoinSetSelectSingle p ps _qid q -> onlyWrap
    $ S (fmap3 snd $ foldl' And p ps) q
  JoinSetProduct {} -> mzero
  JoinSetAtLeast2 ps l0 r0 qs -> fmap (foldl1 combFuzz) $ runListT $ do
    (ls,rs) <- mkListT $ return $ foldl' onEitherSide [(pure l0,pure r0)] qs
    let SepProps {..} = mkSepProps (fst <$> ls) (fst <$> rs) ps
    -- Build the tunnels.
    let canSel = spLeftNonEq <> spRightNonEq <> spConnNonEq
        mustJoin = spConnEq
        pl = spLeftEq
        pr = spRightEq
    simplestLeft <- lift $ recur $ mkJoinSet pl ls
    simplestRight <- lift $ recur $ mkJoinSet pr rs
    optimalLeft <- lift $ recur $ mkJoinSet pr rs
    optimalRight <- lift $ recur $ mkJoinSet pr rs
    let simplestJoin = joinQ (TExit echoId) mustJoin simplestLeft simplestRight
        optimalJoin = joinQ (TEntry echoId) mustJoin optimalLeft optimalRight
    return $ selQ (TEntry echoId) canSel simplestJoin `combFuzz` optimalJoin
  _ -> error "unreachable"
  where
    onlyWrap q =
      wrapFuzzy NoEcho . fmap (either return id) <$> traverse2 recur q

joinPermutations
  :: Hashables2 e s
  => SymEmbedding e0 s e
  -> Query e s
  -> Maybe (FuzzyQuery e s)
joinPermutations emb q =
  (`evalStateT` def)
  $ cachedJoin (echoingJoins $ fix $ \f -> echoingJoins f def)
  $ queryToJoinSet emb q
