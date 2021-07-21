{-# LANGUAGE PatternSynonyms #-}
module Data.Query.Optimizations.EchoingJoins () where


import           Control.Applicative
import           Control.Monad.Identity
import           Control.Monad.State
import           Control.Utils.Free
import           Data.Foldable
import           Data.Functor.Compose
import qualified Data.HashMap.Strict            as HM
import           Data.List
import qualified Data.List.NonEmpty             as NEL
import           Data.Query.Algebra
import           Data.Query.Optimizations.Echo
import           Data.Query.Optimizations.Types
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           GHC.Generics
type L0 = []
type L1 = NEL.NonEmpty
data L2 a = L2 a (L1 a)
  deriving (Functor,Traversable,Foldable)
instance Semigroup (L2 a) where
  L2 a (a' NEL.:| as) <> bs = L2 a $ a' NEL.:| as ++ toList bs
type QId = Int

type FuzzyQuery e s = Free (Compose L1 (TQuery e)) s
wrapFuzzy :: EchoSide -> Query e (FuzzyQuery e s) -> FuzzyQuery e s
wrapFuzzy e x =
  FreeT
  $ Identity
  $ Free
  $ Compose
  $ return
  $ TQuery { tqQuery = Right x,tqEcho = e }

pattern FuzzyQueryF a = FreeT (Identity (Free (Compose a)))
pattern FuzzyQueryP s = FreeT (Identity (Pure s))

unwrapFuzzy :: FuzzyQuery e s -> Query e0 (TQuery e0 s1)
unwrapFuzzy (FuzzyQueryF a) = _ a
unwrapFuzzy (FuzzyQueryP a) = _ a



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

data JoinSet e s =
  JoinSet { jsProps :: L0 (Prop (Rel (Expr (Maybe QId,e))))
           ,jsQs    :: L1 (QId,Query e (FuzzyQuery e s))
          } deriving Generic

type JProp e = Prop (Rel (Expr (Maybe QId,e)))
-- | Props separated according to left and right.
data SepProps e =
  SepProps
  { spLeftNonEq,spRightNonEq,spLeftEq,spRightEq,spConnEq,spConnNonEq
      :: [JProp e]
  }

mkSepProps :: L1 QId -> L1 QId -> [JProp e] -> SepProps e
mkSepProps lIds rIds ps =   let
  (eqP_,nonEqP_) = partition isEquality ps
  (lEqP,eqP__) = partition (isPushable lIds) eqP_
  (rEqP,connEqP) = partition (isPushable rIds) eqP__
  (lNonEqP,nonEqP__) = partition (isPushable lIds) nonEqP_
  (rsNonEqP,connNonEqP) = partition (isPushable rIds) nonEqP__
  in SepProps {..}


pattern JoinSetSingleQuery qid q =
  JoinSet { jsProps = [],jsQs = (qid,q) NEL.:| [] }
pattern JoinSetSelectSingle p ps qid q =
  JoinSet { jsProps = p:ps,jsQs = (qid,q) NEL.:| [] }
pattern JoinSetProduct qid q qs =
  JoinSet { jsProps = [],jsQs = (qid,q) NEL.:| qs }
pattern JoinSetAtLeast2 ps q0 q1 qs =
  JoinSet { jsProps = ps,jsQs = q0 NEL.:| (q1:qs) }


instance Hashables2 e s => Hashable (JoinSet e s)
data PJState e s =
  PJState { pjsEchoId :: EchoId
           ,pjsCache  :: HM.HashMap (JoinSet e s) (FuzzyQuery e s)
          }

cachedJoin
  :: (JoinSet e s -> EchoId -> State (PJState e s) (FuzzyQuery e s))
  -> JoinSet e s
  -> State (PJState e s) (FuzzyQuery e s)
cachedJoin f js = do
  gets (HM.lookup js . pjsCache) >>= \case
    Just x -> return x
    Nothing -> do
      tid <- gets pjsEchoId
      modify $ \pjs -> pjs { pjsEchoId = pjsEchoId pjs + 1 }
      -- Don't worry that we are not reserving the return value on the
      -- cache while evaluating.
      ret <- f js tid
      modify $ \pjs -> pjs { pjsCache = HM.insert js ret $ pjsCache pjs }
      return ret

echoingJoins
  :: Monad m
  => (JoinSet e s -> m (FuzzyQuery e s))
  -> EchoId
  -> JoinSet e s
  -> m (FuzzyQuery e s)
echoingJoins _ _ (JoinSetSingleQuery _qid q) = return $ wrapFuzzy NoEcho q
echoingJoins _ _ (JoinSetSelectSingle p ps _qid q) =
  return $ wrapFuzzy NoEcho $ S (fmap3 snd $ foldl And p ps) q
echoingJoins _ _ JoinSetProduct {} = mzero
echoingJoins recur _echoId (JoinSetAtLeast2 ps l0 r0 qs) =
  fmap (foldl1 combFuzz) $ runListT $ do
    (ls,rs) <- mkListT $ return $ foldl' onEitherSide [(pure l0,pure r0)] qs
    let SepProps {..} = mkSepProps (fst <$> ls) (fst <$> rs) ps
    -- Build the tunnels.
    tid <- gets pjsEchoId
    modify $ \pjs -> pjs { pjsEchoId = pjsEchoId pjs + 1 }
    let tunnelSrc q = TQuery { tqQuery = Left q,tqEcho = TEntry tid }
        tunnelDst q = TQuery { tqQuery = Left q,tqEcho = TExit tid }
    let canSel = spLeftNonEq <> spRightNonEq <> spConnNonEq
        mustJoin = spConnEq
        pl = spLeftEq
        pr = spRightEq
    lq <- lift $ recur $ JoinSet pl ls
    rq <- lift $ recur $ JoinSet pr rs
    let simplestJoin = joinQ (TExit tid) mustJoin rq lq
    let joinP = foldl1 And $ canSel <> mustJoin
    return $ selQ (TEntry tid) canSel $ _ simplestJoin -- <+> tunnelSrc (joinQ (And selP joinP) rq lq)
  where
    selQ :: EchoSide -> [JProp e] -> FuzzyQuery e s -> FuzzyQuery e s
    selQ _ p q = if null p then q else return $ _ $ S (foldl1' And p) q
    wrapFuzz :: ListT (State EchoId) (FuzzyQuery e s)
             -> State EchoId (TQuery (Maybe QId,e) s0)
    wrapFuzz = error "not implemented"
    joinQ :: EchoSide
          -> [JProp e]
          -> FuzzyQuery e s
          -> FuzzyQuery e s
          -> FuzzyQuery e s
    joinQ _ p q1 q2 =
      J (foldl1' And p) q1 q2
      `combFuzz` pushedLR (connNonEqP,connEqP) (lEqP <> lNonEqP) (rEqP <> rNonEqP)

(<+>) :: Alternative  m => a  -> a -> m a
a <+> b = return a <|> return b

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
isPushable = error "not implemented"


isEquality :: Prop (Rel (Expr e')) -> Bool
isEquality = error "not implemented"

--
-- Finally we create echo tunnels between each of the resulting
-- queries and the pure equijoin query. This is a heuristic that
-- assumes that the particular query is useful to be considered when
-- considering andy of the queries in question.

enumerate :: L1 a -> L1 (QId,a)
enumerate (x NEL.:| xs) = (0,x) NEL.:| zip [1..] xs

onEitherSide :: L0 (L1 a,L1 a) -> a -> L0 (L1 a,L1 a)
onEitherSide res q = do
  (ls,rs) <- res
  [(pure q <> ls,rs),(ls,pure q <> rs)]
