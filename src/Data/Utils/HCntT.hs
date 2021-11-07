{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE LambdaCase                #-}
{-# LANGUAGE QuantifiedConstraints     #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE UndecidableInstances      #-}
{-# OPTIONS_GHC -O2 -fno-prof-count-entries -fno-prof-auto #-}
module Data.Utils.HCntT
  (HCntT
  ,(<//>)
  ,dissolve
  ,CHeap
  ,IsHeap(..)
  ,MonadHalt(..)) where

import           Control.Applicative
import           Control.Monad.Cont
import           Control.Monad.Except
import           Control.Monad.Morph
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Writer
import qualified Data.Heap            as H
import qualified Data.IntMap.Strict   as IM
import qualified Data.List.NonEmpty   as NEL
import           Data.Proxy
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.ListT
import           Data.Utils.MinElem
import           Data.Utils.Nat
import           Data.Utils.Tup
import           GHC.Generics


-- | A stable heap: items with the same key are returned in the order
-- they were inserted. The HeapKey mempty is less than all other
-- heapkeys.
class (forall v . Monoid (h v),Functor h,Ord (HeapKey h))
  => IsHeap h where
  type HeapKey h :: *

  popHeap :: h v -> Maybe ((HeapKey h,v),h v)
  singletonHeap :: HeapKey h -> v -> h v
  maxKeyHeap :: h v -> Maybe (HeapKey h)

data CHeap k a = CHeap { chHeap :: H.Heap (H.Entry k a),chMax :: Maybe k }

instance Ord k => Semigroup (CHeap k a) where
  ch <> ch' =
    CHeap
    { chHeap = chHeap ch <> chHeap ch',chMax = case (chMax ch,chMax ch) of
      (Nothing,Nothing) -> Nothing
      (Just a,Nothing)  -> Just a
      (Nothing,Just a)  -> Just a
      (Just a,Just a')  -> Just $ max a a' }
instance Ord k =>  Monoid (CHeap k a) where
  mempty = CHeap { chHeap = mempty,chMax = Nothing }

instance Ord k => Functor (CHeap k) where
  fmap f ch =
    CHeap { chHeap = H.mapMonotonic (fmap f) $ chHeap ch,chMax = chMax ch }

instance Ord k => IsHeap (CHeap k) where
  type HeapKey (CHeap k) = k
  popHeap CHeap {..} = do
    (H.Entry k v,h) <- H.viewMin chHeap
    let m = if null h then Nothing else chMax
    return ((k,v),CHeap { chMax = m,chHeap = h })
  singletonHeap k v =
    CHeap { chMax = Just k,chHeap = H.singleton $ H.Entry k v }
  maxKeyHeap = chMax

-- | The heap priority. This is mostly to make the typechecker happy
-- when the ambiguity stemming from the type family HeapKey confuses
-- GHC.
newtype HPrio h = HPrio { getHPrio :: HeapKey h }

newtype IntMMap a = IntMMap { unIntMMap :: IM.IntMap (NEL.NonEmpty a) }
  deriving (Show,Functor,Foldable)

instance Semigroup (IntMMap a) where
  IntMMap a <> IntMMap b = IntMMap $ IM.unionWith (<>) a b
instance Monoid (IntMMap a) where
  mempty = IntMMap mempty

newtype NonNeg a = NonNeg { getNonNeg :: a }
  deriving (Show,Eq,Ord)
instance (Ord a,Zero a) =>  Zero (NonNeg a) where
  zero = NonNeg zero

instance (Ord a,Zero a) => MinElem (NonNeg a) where
  minElem = zero

instance IsHeap IntMMap where
  type HeapKey IntMMap = NonNeg IM.Key
  popHeap (IntMMap m) = do
    ((k, h NEL.:| xs0),rest) <- IM.minViewWithKey m
    return ((NonNeg k,h), IntMMap $ case NEL.nonEmpty xs0 of
      Nothing -> rest
      Just xs -> IM.insert k xs rest)
  singletonHeap k v = IntMMap $ IM.singleton (getNonNeg k) (pure v)
  maxKeyHeap (IntMMap im) = NonNeg . fst . fst <$> IM.maxViewWithKey im

-- | A list is a heap where they key is a unit.
instance IsHeap [] where
  type HeapKey [] = ()
  popHeap []     = Nothing
  popHeap (v:vs) = Just (((),v),vs)
  singletonHeap () = pure
  maxKeyHeap [] = Nothing
  maxKeyHeap _  = Just ()

-- | An intermediate layer of the continuation. Returning empty
-- results should be passing. It contains a heap of intermediate
-- values and maybe a value, if there is no value then we may have
-- pending eithers that we need to check the next set of values for a
-- result. Run the either and get a new result.
--
-- We use Tup2 to be able to be flexible on whether we use BFS or DFS
-- search among the branches. While this is a weighted search, it
-- remains a question of how the options that have the same value
-- should be ordered. While it is more commonsensical to use BFS,
-- especially since most problems have fairly wide branching.
newtype HRes h m r = HRes (Tup2 (h (Brnch h r m)),[r])



-- | A computation branch is simply a computation that evalautes to
-- HRes. The computation must be able to interact with CompState as
-- required by the infrastructure supporting <//>. Note that
type BrnchM h r m = ReaderT (HPrio h) (StateT (CompState h r m) m)
type Brnch h r m = BrnchM h r m (HRes h m r)

emptyHRes :: (forall v . Monoid (h v)) => HRes h m r
emptyHRes = HRes (Tup2 mempty mempty,[])

newtype HCntT h r m a =
  HCntT { unHCntT :: (a -> BrnchM h r m (HRes h m r))
                  -> BrnchM h r m (HRes h m r)
        }

instance Functor (HCntT h r m) where
  fmap f m = HCntT $ \c -> unHCntT m (c . f)
  {-# INLINE fmap #-}
instance Functor m => Applicative (HCntT v r m) where
  pure x  = HCntT ($ x)
  {-# INLINE pure #-}
  f <*> v = HCntT $ \c -> unHCntT f $ \g -> unHCntT v (c . g)
  {-# INLINE (<*>) #-}
  m *> k = m >>= const k
  {-# INLINE (*>) #-}
instance Functor m => Monad (HCntT v r m) where
  m >>= k  = HCntT $ \ c -> unHCntT m (\ x -> unHCntT (k x) c)
  {-# INLINE (>>=) #-}
instance MonadTrans (HCntT v r) where
  lift m = HCntT (lift (lift m) >>=)
  {-# INLINE lift #-}

dissolve
  :: (MinElem (HeapKey h),IsHeap h,Monad m) => HCntT h r m r -> ListT m r
dissolve = dissolveI (\x -> pure $ HRes (Tup2 mempty mempty,[x]))

-- | By the /disolution/ we mean the process of turning an HCntT value
-- to a ListT value. The ListT will lazily produce just as many
-- results as are required, and of course just as many effects as
-- required, and not more. This indicates that the process of building
-- comptuatins is composable but not incremental. The price of <//> is
-- that, unlike other frameworks, once we start drawing results from
-- the computation we can not apply further constraints on the
-- resulting objects. For example in a computation built using ListT
-- we can take the first couple of results and then use the rest of in
-- a different computation.
--
-- The way disolution works then is to first commit to the computation
-- constructed by applying the continuation obtaining an HRes. If HRes
-- presents a concret result it is yielded into ListT. Otherwise or
-- when all the results have been yielded.
dissolveI
  :: forall h m r x .
  (IsHeap h,Monad m,MinElem (HeapKey h))
  => (x -> m (HRes h m r))
  -> HCntT h r m x
  -> ListT m r
dissolveI fin c = ListT $ do
  (HRes (Tup2 hl hr,rs),st) <- runStateT
    (runReaderT (unHCntT c $ lift2 . fin) (HPrio minElem))
    def
  go st (hl <> hr,rs)
  where
    go :: CompState h r m -> (h (Brnch h r m),[r]) -> m (Maybe (r,ListT m r))
    go st = \case
      (h,r:rs) -> return $ Just (r,ListT $ go st (h,rs))
      (h,[]) -> do
        case popHeap h of
          Nothing -> return Nothing
          Just ((v',c'),h') -> do
            (HRes (Tup2 hl hr,rs),st')
              <- runStateT (runReaderT c' (HPrio v')) st
            -- h1 is the the last subbag. If it is appended on the
            -- left hand it is a DFS, if on the right it is a BFS. If
            -- this weren't weighted we would want it to be
            -- BFS. However we need the fallback markers to be appended
            -- from the right. We reconcile this problem by appending
            -- on both sides.
            go st' (hl <> h' <> hr,rs)

instance (IsHeap h,MinElem (HeapKey h),Monad m)
  => Alternative (HCntT h r m) where
  -- | Split simply introduces two high priority elements in the
  -- heap. Because the heap is stable, the last one in is the last one
  -- out, this is fundamentally a BFS search. This is not ideal for
  -- problems that produce wide search trees. This behavior hinges on
  -- dissolve pushing concatenating new trees on the right hand side
  -- of the accumulated heap.
  HCntT m <|> HCntT m' = HCntT $ \f -> return
    $ HRes (Tup2 (h (m f) <> h (m' f)) mempty,[])
    where
      h = singletonHeap minElem
  empty = HCntT $ const $ return emptyHRes

instance (IsHeap h,MinElem (HeapKey h),Monad m) => MonadPlus (HCntT h r m) where
  mplus = (<|>)
  mzero = empty

class Monad m => MonadHalt m where
  type HaltKey m :: *
  halt :: HaltKey m -> m ()
  once :: m a -> m a

instance MonadHalt m => MonadHalt (StateT s m) where
  type HaltKey (StateT s m) =  HaltKey m
  halt = lift . halt
  once = hoist once
instance MonadHalt m => MonadHalt (ReaderT r m) where
  type HaltKey (ReaderT r m) =  HaltKey m
  halt = lift . halt
  once = hoist once
instance (Monoid w,MonadHalt m) => MonadHalt (WriterT w m) where
  type HaltKey (WriterT w m) = HaltKey m
  halt = lift . halt
  once = hoist once
instance MonadHalt m => MonadHalt (ExceptT e m) where
  type HaltKey (ExceptT e m) = HaltKey m
  halt = lift . halt
  once = hoist once

instance (Monad m,IsHeap h) => MonadHalt (HCntT h r m) where
  type HaltKey (HCntT h r m) = HeapKey h
  once = nested (\_h r _rs -> HRes (Tup2 mempty mempty,[r])) emptyHRes
  halt v = HCntT $ \nxt -> do
    v' <- asks $ min v . getHPrio
    return $ HRes (Tup2 (singletonHeap v' $ nxt ()) mempty,[])



-- | The <//> (pronounced eitherl) does runs the left hand side
-- operator and if no values are produced in the ENTIRE computation
-- based on that, only then does it try to evaluate the right hand
-- side. HCntT does not guarante that the right hand side will be
-- scheduled immediately after it realizes that there are no
-- solutions, but it does guarantee that it will be scheduled before
-- it moves on to a new cost cost value. In other words it is only
-- guarantted that the priority of the fallback branch will tie the
-- last failing branch of the left hand side in terms of priority, but
-- if there are other branches that tie, there is no guarantee of how
-- those will be scheduled.
--
-- There are two challenges that our particular feature set imposes to
-- implementing this:
--
-- - We want <//> to operate on the entire operation, in other words
--   it is not like Oleg Kiselyov's msplit.
--
-- - We are operating in the context of a weihtend search. This means
--   that control is likely to escape a branch that passes through the
--   left hand side operator before it is exhausted.
--
-- We solve these problems with the use of a special kind of branch we
-- call a marker and a global state. Since branches are arbitrary
-- processes we equip them with global lookup table (~CompState~) of
-- fallback branches. Each marker corresponds to a location in the
-- lookup table and each valid location references one of the markers
-- to which it corresponds, specifically the final one. When it is
-- scheduled the marker looks up the fallback branch in the table and
-- checks if the correspondence is mutual. There are 3 scenaria that
-- may play out at this point:
--
-- - The fallback location has been invalidated by a branch that
--   yielded a result.
--
-- - The fallback location is valid but does not correspond to the
--   scheduled marker. This means due to the left hand side branch
--   spawning expensive sub-branches, another marker has been inserted
--   to (possibly) trigger the fallback.
--
-- - The fallback location is valid and corresponds to the scheduled
--   marker. This means that the fallback needs to be run and
--   invalidated.
--
-- When an exprssion A <//> B appears we create a new fallback entry
-- in the lookup table containing B and recursively "infect" all
-- spawned branches under A to perform two actions immediately after
-- they generate new branches and results:
--
-- - If there is at least one valid result invaludate the fallback in
--   the lookup table and stop infecting child branches with the
--   currently  described hook
--
-- - If the fallback is invalid it means there have already been valid
--   results that rendered the fallback obsolete. Stop infecting
--   sub-branches.
--
-- - If none of above happened check the priority of the last marker
--   corresponding to the fallback (registerd in the lookup table
--   entry). If it is strictly lower than the lowest priority
--   subbranch do nothing because there is a well placed marker to
--   handle it. Otherwise create a new marker of the same priority as
--   the lowest priority subbranch and put it in the right-append
--   heap. This way we know it will be scheduled AFTER the last branch
--   relating to the fallback.
--
-- One may think of the scheduler sequence of characters (branches), a
-- containing a <//> being an opening parenthesis and a marker with
-- mutual correspondence to the fallback being a closing
-- parenthesis. We insert new markers when new branches appear that
-- should be between the "parentheses" and invalidate the old
-- parenthesis by asserting mutual correspondence between fallback and
-- marker..
--
-- If appropriate register an either and insert a corresponding marker
-- at the end of the heap.
(<//>) :: (Monad m,IsHeap h) => HCntT h r m a -> HCntT h r m a -> HCntT h r m a
HCntT c <//> e = HCntT $ \nxt -> do
  c nxt >>= \case
    r@(HRes (_,_:_)) -> return r
    (HRes (h,[])) -> case maxKeyHeap2 h of
      Nothing -> unHCntT e nxt
      Just bnd -> do
        (eithId,mrk) <- mkMarker bnd $ unHCntT e nxt
        return $ beforeFallback eithId h bnd mrk

type EitherId = Int
type MrkId = Int
type Fallback h r m = Brnch h r m
type Marker h r m = Brnch h r m

-- | The bound of either. Nothing means that everything exceeds this
-- bound because the either was not yet bounded.
type EitherBnd h = HeapKey h

-- | Check if a bound is further then the either. To be safe we
-- consider equality to be exceeding.
exceedsEitherBnd :: IsHeap h => Proxy h -> HeapKey h -> EitherBnd h -> Bool
exceedsEitherBnd Proxy bnd ebnd = bnd >= ebnd

data FallbackEntry h r m =
  FallbackEntry
  { eleLastMrk :: MrkId
   ,eleProc    :: Fallback h r m
   ,eleBound   :: HeapKey h -- Nothing means minimum bound.
  }

newEle :: HeapKey h -> MrkId -> Fallback h r m -> FallbackEntry h r m
newEle k mrkId c = FallbackEntry { eleLastMrk = mrkId,eleProc = c,eleBound = k }

data CompState h r m =
  CompState
  { csEitherSet :: IM.IntMap (FallbackEntry h r m) -- An either and the latest marker
   ,csEitherUid :: Int
   ,csMrkUid    :: Int
  } deriving Generic
instance Default (CompState h r m)

getEitherId :: Monad m => BrnchM h r m Int
getEitherId = do
  cs <- get
  put $ cs { csEitherUid = csEitherUid cs + 1 }
  return $ csEitherUid cs
getMrkId :: Monad m => BrnchM h r m Int
getMrkId = do
  cs <- get
  put $ cs { csEitherUid = csEitherUid cs + 1 }
  return $ csEitherUid cs

putFallback
  :: Monad m
  => EitherBnd h
  -> EitherId
  -> MrkId
  -> Fallback h r m
  -> BrnchM h r m ()
putFallback b i mrkId c = modify $ \cs -> cs
  { csEitherSet = IM.insert i (newEle b mrkId c) $ csEitherSet cs }

dropFallback :: Monad m => MrkId -> BrnchM h r m ()
dropFallback
  i = modify $ \cs -> cs { csEitherSet = IM.delete i $ csEitherSet cs }

-- | A marker looks up the either and checks if the latest marker
-- field matches its own. If it does then it removes the either from
-- the map and runs it. Otherwise the marker simply evaluates to to a
-- bottom..
newMarker :: (Monad m,IsHeap h) => EitherId -> MrkId -> Marker h r m
newMarker eithId mrkId = do
  k <- gets $ IM.lookup eithId . csEitherSet
  case k of
    Just FallbackEntry {..} -> if eleLastMrk
      == mrkId then dropFallback eithId >> eleProc else return emptyHRes
    -- The Fallback was removed because there was a result
    Nothing -> return emptyHRes

-- | Register an Fallback and return a marker pointing to it.
mkMarker
  :: (Monad m, IsHeap h)
  => EitherBnd h
  -> Fallback h r m
  -> BrnchM h r m (EitherId,Marker h r m)
mkMarker b clause = do
  mrkId <- getMrkId
  eithId <- getEitherId
  putFallback b eithId mrkId clause
  return (eithId,newMarker eithId mrkId)

-- | We already know that the bound exceeds the Fallback bound,
-- Fallback already refers to this marker, this function just fixes
-- the HRes so that the children do update the fallback entry.
beforeFallback
  :: (Monad m,IsHeap h)
  => EitherId
  -> Tup2 (h (Brnch h r m))
  -> HeapKey h
  -> Marker h r m
  -> HRes h m r
beforeFallback eithId (Tup2 hl hr) bnd mrk =
  HRes (fmap2 (regFallback eithId) $ Tup2 hl (hr <> singletonHeap bnd mrk),[])

-- | If appropriate register an either and insert a corresponding
-- marker at the end of the heap. This function delegates to mkMarker'
-- to update the status of the either field according to the marker.
regFallback :: (Monad m,IsHeap h) => EitherId -> Brnch h r m -> Brnch h r m
regFallback eid c = c >>= \case
  r@(HRes (_,_:_)) -> return r
  r@(HRes (h,[])) -> case maxKeyHeap2 h of
    Nothing -> return r
    Just bnd -> mkMarker' bnd eid >>= \case
      Nothing  -> return r -- no marker required
      Just mrk -> return $ beforeFallback eid h bnd mrk

maxKeyHeap2 :: IsHeap h => Tup2 (h v) -> Maybe (HeapKey h)
maxKeyHeap2 (Tup2 hl hr) = case (maxKeyHeap hl,maxKeyHeap hr) of
  (Nothing,Nothing) -> Nothing
  (Just l,Nothing)  -> Just l
  (Nothing,Just r)  -> Just r
  (Just l,Just r)   -> Just $ max l r

whenM :: Monad m => Bool -> m a -> m (Maybe a)
whenM b m = if b then Just <$> m else return Nothing

-- | Make a marker for a registered fallback clause and update the
-- latest marker only if the corresponding either exists and the
-- potential marker bound exceeds the either bound.
mkMarker'
  :: forall h r m .
  (Monad m,IsHeap h)
  => HeapKey h
  -> EitherId
  -> BrnchM h r m (Maybe (Marker h r m))
mkMarker' bnd eithId = do
  mrkId <- getMrkId
  ebndM <- gets $ fmap eleBound . IM.lookup eithId . csEitherSet
  case ebndM of
    Nothing
     -> return Nothing -- either was already deprecated. Don't make a marker
    Just ebnd -> whenM (exceedsEitherBnd @h Proxy bnd ebnd) $ do
      modify $ \cs -> cs
        { csEitherSet = IM.adjust (setId mrkId) eithId $ csEitherSet cs }
      return $ newMarker mrkId eithId
  where
    setId :: Int -> FallbackEntry h r m -> FallbackEntry h r m
    setId mrkId ele = ele { eleLastMrk = mrkId,eleBound = bnd }


-- | Once will return at most one variable and then die. It is
-- implemented as a disolution that propagates the halts to the aboive
-- layer. ASSUME THAT THE KEYS HEAP IS INCREASING.
--
-- We assume that once are not deeply nested otherwise it becomes a
-- bubble sort.
nested
  :: forall h m r a .
  (Monad m,IsHeap h)
  => (Tup2 (h (Brnch h r m)) -> r -> [r] -> HRes h m r)
  -> HRes h m r
  -> HCntT h r m a
  -> HCntT h r m a
nested success failCase c = HCntT $ \fin -> go mempty $ unHCntT c fin
  where
    go :: h (Brnch h r m) -> Brnch h r m -> Brnch h r m
    go h b = b <&> \case
      HRes (h',r':rs') -> success h' r' rs'
      HRes (Tup2 hl hr,[]) -> case popHeap $ hl <> h <> hr of
        Nothing          -> failCase
        Just ((k,b'),h') -> HRes (Tup2 (singletonHeap k $ go h' b') mempty,[])

#if 1
-- | Try to open a file. If it doesn't exist just fail. If it exists
-- read the contents
openFile :: (MinElem (HeapKey h),IsHeap h) => String -> HCntT h r IO String
openFile fname = do
  lift $ putStrLn $ "Opening: " ++ fname
  case fname of
    "f1" -> return  "import f2"
    "f2" -> return "import f1"
    _    -> do
      lift $ putStrLn "No such file"
      empty

closeFile :: String -> HCntT v r IO ()
closeFile fname = lift $ putStrLn $ "Closing file: " ++ fname

chooseFile :: (MinElem (HeapKey h),IsHeap h) => [String] -> HCntT h r IO String
chooseFile []     = empty
chooseFile (f:fs) = openFile f <//> (closeFile f >> chooseFile fs)

-- | Try opeining a file, if the file isnt there move on the next one.
-- Once we leave the next.
test :: IO [String]
test = runListT $ dissolve @IntMMap $ do
  txt <- chooseFile ["nonexistent","f2"]
  return ("hello:" ++ txt) <|> return ("bye:" ++ txt)

stream :: Monad m => HCntT IntMMap r m Int
stream = go 0 where
  go i = do
    halt $ NonNeg i
    return i <|> go (i+1)

-- > test2
-- [(5,5,0),(5,6,1),(6,4,0),(6,5,1),(6,6,2)]
test2 :: IO [(Int,Int,Int)]
test2 = takeListT 5 $ dissolve @IntMMap  $ do
  (a,b,c) <- (,,) <$> stream <*> stream <*> stream
  guard $ a + b - c == 10
  return (a,b,c)

-- The well known 8 queens problem: How would one position 8 queens on
-- a chess board. This implementation is purposefully naive get many
-- failures and backtracking as possible. It implements no halting so
-- none of the tricks that HCntT supports are implemented it can run
-- with any MonadPlus. We run it with a couple of reasonable
-- backtracking monads to demonstrate that we are not missing much in
-- terms of efficiency.
--
-- Normal haskell list is, as expected, by far the fastest
-- implementation but it is usually too inconvenient to. A more
-- reasonable option is a ListT with a state monad, which is
-- essentially our predicament.
type Sq = (Int,Int)
squares :: MonadPlus m => m Sq
squares = msum [pure (x,y) | x <- [0..7], y <- [0..7]]
qTakes :: Sq -> Sq -> Bool
qTakes (x,y) (x',y') = x == x' || y == y' || x - x' == y - y'
queensN :: MonadPlus m => Int -> m [Sq]
queensN 0 = return [] -- no queens
queensN i = do
  qs <- queensN (i-1)
  q <- squares
  guard $ not $ or [qTakes q q' | q' <- qs]
  return $ q:qs

-- > :set -XTypeApplications
-- > :set +s
-- > import Control.Monad.Identity
--
-- Runnint the queens with HCntT
-- > runIdentity $ takeListT 1 $ dissolve @IntMMap $ queensN 8
-- [[(7,4),(6,1),(5,3),(4,6),(3,7),(2,5),(1,2),(0,0)]]
-- (1.18 secs, 752,059,704 bytes)
--
-- > runIdentity $ (`runStateT` ()) $ takeListT 1 $ queensN 8
-- ([[(7,4),(6,1),(5,3),(4,6),(3,7),(2,5),(1,2),(0,0)]],())
-- (1.14 secs, 675,295,952 bytes)
--
-- Runnint the queens with ListT Identity
-- > runIdentity $ takeListT 1 $ queensN 8
-- [[(7,4),(6,1),(5,3),(4,6),(3,7),(2,5),(1,2),(0,0)]]
-- (0.94 secs, 575,001,120 bytes)
--
-- Runnint the queens with []
-- > take 1 $ queensN 8
-- [[(7,4),(6,1),(5,3),(4,6),(3,7),(2,5),(1,2),(0,0)]]
-- (0.41 secs, 253,386,736 bytes)
#endif
