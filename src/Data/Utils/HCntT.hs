{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE LambdaCase                #-}
{-# LANGUAGE QuantifiedConstraints     #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
module Data.Utils.HCntT (HCntT,eitherl,dissolve) where

import           Control.Applicative
import           Control.Monad.Cont
import           Control.Monad.State
import           Data.Bifunctor
import qualified Data.IntMap.Strict  as IM
import qualified Data.List.NonEmpty  as NEL
import           Data.Proxy
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.ListT
import           GHC.Generics


-- | A stable heap: items with the same key are returned in the order
-- they were inserted.
class (forall v . Monoid (h v),Functor h,Ord (HeapKey h)) => IsHeap h where
  type HeapKey h :: *

  popHeap :: h v -> Maybe ((HeapKey h,v),h v)
  singletonHeap :: HeapKey h -> v -> h v
  maxKeyHeap :: h v -> Maybe (HeapKey h)

newtype IntMMap a = IntMMap { unIntMMap :: IM.IntMap (NEL.NonEmpty a) }
  deriving (Show,Functor)

instance Semigroup (IntMMap a) where
  IntMMap a <> IntMMap b = IntMMap $ IM.unionWith (<>) a b
instance Monoid (IntMMap a) where
  mempty = IntMMap mempty

instance IsHeap IntMMap where
  type HeapKey IntMMap = IM.Key
  popHeap (IntMMap m) = bimap (fmap NEL.head) IntMMap <$> IM.minViewWithKey m
  singletonHeap k v = IntMMap $ IM.singleton k (pure v)
  maxKeyHeap (IntMMap im) = fst . fst <$> IM.maxViewWithKey im

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
-- result. Run the either and get a new result
type Brnch h r m = StateT (CompState h r m) m (HRes h m r)
newtype HRes h m r = HRes (h (Brnch h r m),[r])

emptyHRes :: (forall v . Monoid (h v)) => HRes h m r
emptyHRes = HRes (mempty,[])

-- | Continuation based monad that can either jump to a conclusion.
type HCntT h r m = ContT (HRes h m r) (StateT (CompState h r m) m)

dissolve :: (IsHeap h,Monad m) => HCntT h r m r -> ListT m r
dissolve = dissolveI (\x -> pure $ HRes (mempty,[x]))

dissolveI
  :: forall h m r x .
  (IsHeap h,Monad m)
  => (x -> m (HRes h m r))
  -> HCntT h r m x
  -> ListT m r
dissolveI fin c = ListT $ do
  (HRes r,st) <- runStateT (runContT c $ lift . fin) def
  go st r
  where
    go :: CompState h r m -> (h (Brnch h r m),[r]) -> m (Maybe (r,ListT m r))
    go st = \case
      (h,r:rs) -> return $ Just (r,ListT $ go st (h,rs))
      (h,[]) -> case popHeap h of
        Nothing -> return Nothing
        Just ((_,c'),h') -> do
          (HRes (h1,rs),st') <- runStateT c' st
          go st' (h' <> h1,rs)

instance (forall v . Monoid (h v),Monad m) => Alternative (HCntT h r m) where
  ContT m <|> ContT m' = ContT $ \f -> do
    HRes (h,r) <- m f
    HRes (h',r') <- m' f
    return $ HRes (h <> h',r <> r')
  empty = ContT $ const $ return emptyHRes


-- | If appropriate register an either and insert a corresponding
-- marker at the end of the heap.
eitherl
  :: (Monad m,IsHeap h) => HCntT h r m a -> HCntT h r m a -> HCntT h r m a
eitherl (ContT c) e = ContT $ \nxt -> do
  c nxt >>= \case
    r@(HRes (_,_:_)) -> return r
    (HRes (heap,[])) -> case maxKeyHeap heap of
      Nothing -> runContT e nxt
      Just bnd -> do
        (eithId,mrk) <- mkMarker bnd $ runContT e nxt
        return $ beforeEitherl eithId heap bnd mrk


type EitherId = Int
type MrkId = Int
type EitherL h r m = Brnch h r m
type Marker h r m = Brnch h r m

-- | The bound of either. Nothing means that everything exceeds this
-- bound because the either was not yet bounded.
type EitherBnd h = HeapKey h

-- | Check if a bound is further then the either. To be safe we
-- consider equality to be exceeding.
exceedsEitherBnd :: IsHeap h => Proxy h -> HeapKey h -> EitherBnd h -> Bool
exceedsEitherBnd Proxy bnd ebnd = bnd >= ebnd

data EitherLEntry h r m =
  EitherLEntry
  { eleLastMrk :: MrkId
   ,eleProc    :: EitherL h r m
   ,eleBound   :: HeapKey h -- Nothing means minimum bound.
  }

newEle :: HeapKey h -> MrkId -> EitherL h r m -> EitherLEntry h r m
newEle k mrkId c = EitherLEntry { eleLastMrk = mrkId,eleProc = c,eleBound = k }

data CompState h r m =
  CompState
  { csEitherSet :: IM.IntMap (EitherLEntry h r m) -- An either and the latest marker
   ,csEitherUid :: Int
   ,csMrkUid    :: Int
  } deriving Generic
instance Default (CompState h r m)

getEitherId :: Monad m => StateT (CompState h r m0) m Int
getEitherId = do
  cs <- get
  put $ cs { csEitherUid = csEitherUid cs + 1 }
  return $ csEitherUid cs
getMrkId :: Monad m => StateT (CompState h r m0) m Int
getMrkId = do
  cs <- get
  put $ cs { csEitherUid = csEitherUid cs + 1 }
  return $ csEitherUid cs

putEitherl
  :: MonadState (CompState h r m0) m
  => EitherBnd h
  -> EitherId
  -> MrkId
  -> EitherL h r m0
  -> m ()
putEitherl b i mrkId c = modify $ \cs -> cs
  { csEitherSet = IM.insert i (newEle b mrkId c) $ csEitherSet cs }

dropEitherl :: MonadState (CompState h r m0) m => MrkId -> m ()
dropEitherl
  i = modify $ \cs -> cs { csEitherSet = IM.delete i $ csEitherSet cs }

-- | A marker looks up the either and checks if the latest marker
-- field matches its own. If it does then it removes the either from
-- the map and runs it. Otherwise the marker simply evaluates to to a
-- bottom..
newMarker :: (Monad m,IsHeap h) => EitherId -> MrkId -> Marker h r m
newMarker eithId mrkId = do
  k <- gets $ IM.lookup eithId . csEitherSet
  case k of
    Just EitherLEntry {..} -> if eleLastMrk == mrkId
      then dropEitherl eithId >> eleProc else return emptyHRes
    -- The eitherl was removed because there was a result
    Nothing -> return emptyHRes

-- | Register an eitherl and return a marker pointing to it.
mkMarker
  :: (Monad m, IsHeap h)
  => EitherBnd h
  -> EitherL h r m
  -> StateT (CompState h r m) m (EitherId,Marker h r m)
mkMarker b clause = do
  mrkId <- getMrkId
  eithId <- getEitherId
  putEitherl b eithId mrkId clause
  return (eithId,newMarker eithId mrkId)

-- | We already know that the bound exceeds the eitherl bound, eitherl
-- already refers to this marker, this function just fixes the HRes.
beforeEitherl
  :: (Monad m,IsHeap h)
  => EitherId
  -> h (Brnch h r m)
  -> HeapKey h
  -> Marker h r m
  -> HRes h m r
beforeEitherl eithId heap bnd mrk =
  HRes (fmap (regEither eithId) $ heap <> singletonHeap bnd mrk,[])

-- | If appropriate register an either and insert a corresponding
-- marker at the end of the heap. This function delegates to mkMarker'
-- to update the status of the either field according to the marker.
regEither :: (Monad m,IsHeap h) => EitherId -> Brnch h r m -> Brnch h r m
regEither eid c = c >>= \case
  r@(HRes (_,_:_)) -> return r
  r@(HRes (heap,[])) -> case maxKeyHeap heap of
    Nothing -> return r
    Just bnd -> mkMarker' bnd eid >>= \case
      Nothing  -> return r -- no marker required
      Just mrk -> return $ beforeEitherl eid heap bnd mrk

whenM :: Monad m => Bool -> m a -> m (Maybe a)
whenM b m = if b then Just <$> m else return Nothing

-- | Make a marker for a registered eitherl clause and update the
-- latest marker only if the corresponding either exists and the
-- potential marker bound exceeds the either bound.
mkMarker'
  :: forall h r m .
  (Monad m,IsHeap h)
  => HeapKey h
  -> EitherId
  -> StateT (CompState h r m) m (Maybe (Marker h r m))
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
    setId :: Int -> EitherLEntry h r m -> EitherLEntry h r m
    setId mrkId ele = ele { eleLastMrk = mrkId,eleBound = bnd }


#if 1

openFile :: IsHeap v => String -> HCntT v r IO String
openFile fname = do
  lift2 $ putStrLn $ "Opening: " ++ fname
  case fname of
    "f1" -> return  "the file is f1"
    "f2" -> return "(a (sexp))"
    _    -> do
      lift2 $ putStrLn "No such file"
      empty

closeFile :: String -> HCntT v r IO ()
closeFile fname = lift2 $ putStrLn $ "Closing file: " ++ fname

chooseFile :: IsHeap v => [String] -> HCntT v r IO String
chooseFile []     = empty
chooseFile (f:fs) = openFile f `eitherl` (closeFile f >> chooseFile fs)

test :: IO [String]
test = runListT $ dissolve @IntMMap $ do
  txt <- chooseFile ["f1","nonexistent","f2"]
  return ("hello:" ++ x) <|> return ("bye:" ++ x)
#endif
