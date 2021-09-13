{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE RoleAnnotations       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}
module Control.Antisthenis.Bool
  (interpretBExp
  ,BoolOp(..)
  ,GBool(..)
  ,And
  ,Or
  ,BoolTag
  ,Exists(..)
  ,orToAndConv
  ,andToOrConv) where

import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.AssocContainer
import           Control.Antisthenis.Convert
import           Control.Antisthenis.Test
import           Control.Antisthenis.Types
import           Control.Antisthenis.VarMap
import           Control.Antisthenis.Zipper
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Utils.Free
import           Data.Coerce
import           Data.Foldable
import qualified Data.IntMap                                as IM
import qualified Data.IntSet                                as IS
import qualified Data.List.NonEmpty                         as NEL
import           Data.Maybe
import           Data.Profunctor
import           Data.Proxy
import           Data.Utils.AShow
import           Data.Utils.Default
import           GHC.Generics

-- The bound here is a pair of values which are monotonically
-- increasing functions to the MINIMUM number of steps required if the
-- result is True and the Minimum number of steps if the final result
-- is false.
--
-- At each step we evaluate the process that would minimize our
-- result. thre resut is either a) the steps required a to reach the
-- absorbing element (AE) or b) the number of steps all the processes
-- will need to reach the non-absrbing element.
--
-- In any case while running we want to shift strategies when the one
-- we are after stops being beneficial. This is reified by the cap on
-- steps we enforce on computing the process. Here the cap has a bit
-- more structure than just a less-than operator. The bound returned
-- on the absorbing element side must not exceed the absorbing element
-- bound of the second most promising process, and on the
-- non-absorbing element side it must not exceed the size.
--
-- The logic of enforement is in the argument of mkMachine.

newtype Cost = Cost Integer
  deriving (Generic,Eq)
instance AShow Cost
instance Default Cost where
  def = Cost 0
instance Ord Cost where
  compare (Cost a) (Cost b) = compare a b
instance Semigroup Cost where
  Cost a <> Cost b = Cost $ a + b
instance Monoid Cost where
  mempty = Cost 0

-- | At least one.
newtype Exists = Exists { unExists :: Bool} deriving Generic
instance AShow Exists
instance Semigroup Exists where
  Exists a <> Exists b = Exists $ a || b
instance Monoid Exists where
  mempty = Exists False
instance Default Exists where
  def = Exists False

data GAbsorbing v = GAbsorbing { gaAbsorbing :: v,gaNonAbsorbing :: v }
data GBool op v = GBool { gbTrue :: v,gbFalse :: v }
  deriving (Eq,Generic)
instance AShow v => AShow  (GBool op v)
type BoolBound op = GBool op Cost
type BoolV op = GBool op Exists

instance Semigroup (GAbsorbing Cost) where
  a <> b =
    GAbsorbing
    { gaAbsorbing = min (gaAbsorbing a) (gaAbsorbing b)
     ,gaNonAbsorbing = gaAbsorbing a <> gaNonAbsorbing b
    }

instance Semigroup a => Semigroup (GBool op a) where
  GBool a b <> GBool a' b' =
    GBool (a <> a') (b <> b')
instance Monoid a => Monoid (GBool op a) where
  mempty = GBool mempty mempty

instance Ord (BoolBound op) where
  compare (GBool a b) (GBool a' b') =
    compare (min a b) (min a' b')

data Or
data And

ifelse :: a -> a -> Bool -> a
ifelse t e p = if p then t else e

data ElemType = AbsorbingElem | NormalElem
elemType :: BoolOp op => GBool op Exists -> ElemType
elemType =
  ifelse AbsorbingElem NormalElem . unExists . gaAbsorbing . toGAbsorbing

-- | Implement it for or:
--
-- non-absorbings  = sum the non-absorbing iterations
-- absorbings  = min of the absorbing iterations
zBound :: BoolOp op
       => Zipper (BoolTag op p) (ArrProc (BoolTag op p) m)
       -> Maybe (BoolBound op)
zBound z = do
  absorb <- assocMinCost $ bgsIts bgs
  return
    $ toGBool
    $ GAbsorbing
    { gaAbsorbing = if null $ bgsInits bgs then gaAbsorbing absorb else Cost 1
     ,gaNonAbsorbing = gaNonAbsorbing absorb
        <> Cost (toInteger (length $ bgsInits bgs))
    }
  where
    bgs = zBgState z

-- | It is finished if the result is absorbing or if the inits and its
-- are empty.
zFinished :: BoolOp op => Zipper (BoolTag op p) (ArrProc (BoolTag op p) m) -> Bool
zFinished z = case zRes z of
  Just (Right x) -> case elemType x of
    AbsorbingElem -> True
    NormalElem    -> False
  _ -> False


class BoolOp op where
  -- | Whether the Bool or error is trumped or trumps the result
  -- depends on the operation.
  toGAbsorbing :: GBool op v -> GAbsorbing v
  toGBool :: GAbsorbing v -> GBool op v

instance BoolOp Or where
  toGAbsorbing
    GBool {..} = GAbsorbing { gaNonAbsorbing = gbFalse,gaAbsorbing = gbTrue }
  {-# INLINE toGAbsorbing #-}
  toGBool
    GAbsorbing {..} = GBool { gbFalse = gaNonAbsorbing,gbTrue = gaAbsorbing }
  {-# INLINE toGBool #-}

instance BoolOp And where
  toGAbsorbing
    GBool {..} = GAbsorbing { gaNonAbsorbing = gbTrue,gaAbsorbing = gbFalse }
  {-# INLINE toGAbsorbing #-}
  toGBool
    GAbsorbing {..} = GBool { gbTrue = gaNonAbsorbing,gbFalse = gaAbsorbing }
  {-# INLINE toGBool #-}

-- | Combine a local result with a partial resut. Bounded result would
-- never.
combBoolBndR
  :: BoolOp op
  => BndR (BoolTag op p)
  -> Either (ZErr (BoolTag op p)) (ZRes (BoolTag op p))
  -> Either (ZErr (BoolTag op p)) (ZRes (BoolTag op p))
combBoolBndR newRes oldRes = case (newRes,oldRes) of
  (BndRes r,Left e) -> case elemType r of
    AbsorbingElem -> Right r
    NormalElem    -> Left e
  (BndRes r,Right r') -> Right $ r <> r'
  (BndErr e,Right r) -> case elemType r of
    AbsorbingElem -> Right r
    NormalElem    -> Left e
  (BndErr e,Left _) -> Left e
  (BndBnd _,x) -> x

data CountingAssoc f g a b =
  CountingAssoc
  { assocMinKey :: g a
   ,assocSize   :: !Int
   ,assocData   :: SimpleAssoc f a b
  }
  deriving (Functor,Foldable,Generic)

instance (AShow (f (a,b)),AShow (g a))
  => AShow (CountingAssoc f g a b)

assocMinCost
  :: BoolOp op
  => CountingAssoc [] Maybe (BoolBound op) v
  -> Maybe (GAbsorbing Cost)
assocMinCost assoc =
  let SimpleAssoc l = assocData assoc in case toGAbsorbing . fst <$> l of
    []   -> Nothing
    x:xs -> Just $ foldl' (<>) x xs

instance Ord a => AssocContainer (CountingAssoc [] Maybe a) where
  type KeyAC (CountingAssoc [] Maybe a) = a
  type NonEmptyAC (CountingAssoc [] Maybe a) =
    CountingAssoc NEL.NonEmpty Identity a
  acInsert k v cass =
    CountingAssoc
    { assocMinKey = Identity $ maybe k (min k) $ assocMinKey cass
     ,assocSize = assocSize cass + 1
     ,assocData = acInsert k v $ assocData cass
    }
  acEmpty =
    CountingAssoc { assocMinKey = Nothing,assocSize = 0,assocData = acEmpty }
  acNonEmpty cass = do
    minKey <- assocMinKey cass
    dat <- acNonEmpty $ assocData cass
    return
      CountingAssoc
      { assocMinKey = Identity minKey
       ,assocSize = assocSize cass
       ,assocData = dat
      }
  acUnlift cass =
    CountingAssoc
    { assocMinKey = Just $ runIdentity $ assocMinKey cass
     ,assocSize = assocSize cass
     ,assocData = acUnlift $ assocData cass
    }

data BoolTag op p
instance BndRParams (BoolTag op p) where
  type ZErr (BoolTag op p) = Err
  type ZBnd (BoolTag op p) = BoolBound op
  type ZRes (BoolTag op p) = BoolV op

instance (AShow (ExtCoEpoch p),ExtParams p,BoolOp op)
  => ZipperParams (BoolTag op p) where
  type ZEpoch (BoolTag op p) = ExtEpoch p
  type ZCoEpoch (BoolTag op p) = ExtCoEpoch p
  type ZCap (BoolTag op p) = BoolBound op
  type ZPartialRes (BoolTag op p) =
    Maybe (Either (ZErr (BoolTag op p)) (ZRes (BoolTag op p)))
  type ZItAssoc (BoolTag op p) =
    CountingAssoc [] Maybe (BoolBound op)
  zprocEvolution =
    ZProcEvolution
    { evolutionControl = boolEvolutionControl
     ,evolutionStrategy = boolEvolutionStrategy
     ,evolutionEmptyErr = error "No arguments provided"
    }
  putRes newBnd (partialRes,newZipper) =
    (\() -> maybe newBnd' (Just . combBoolBndR newBnd) partialRes)
    <$> newZipper
    where
      newBnd' = case newBnd of
        BndBnd _ -> Nothing -- The bound will be inserted in the assoc list
        BndRes r -> Just $ Right r
        BndErr e -> Just $ Left e
  -- | The oldBound is certainly a bound.
  replaceRes _oldBnd newBnd (oldRes,newZipper) = do
    oldRes' <- oldRes
    return $ (\() -> Just $ combBoolBndR newBnd oldRes') <$> newZipper
  -- | As a cap use the minimum bound.
  zLocalizeConf coepoch conf z =
    extCombEpochs (Proxy :: Proxy p) coepoch (confEpoch conf)
    $ conf { confCap = maybe (confCap conf) CapVal $ do
      bnd <- assocMinKey $ bgsIts $ zBgState z
      gcap <- case confCap conf of
        CapVal cap -> Just cap
        _          -> Nothing
      return $ min bnd gcap }


boolEvolutionStrategy
  :: Monad m
  => (BndR (BoolTag op p) -> x)
  -> FreeT
    (ItInit
       (ExZipper (BoolTag op p))
       (CountingAssoc [] Maybe (ZBnd (BoolTag op p))))
    m
    (x,BndR (BoolTag op p))
  -> m (x,BndR (BoolTag op p))
boolEvolutionStrategy fin = recur
  where
    recur (FreeT m) = m >>= \case
      Pure a -> return a
      Free f -> case f of
        CmdItInit _it ini -> recur ini
        CmdIt it -> recur
          $ it (error "Unimpl: function to select the cheapest")
        CmdInit ini -> recur ini
        CmdFinished (ExZipper z)
          -> let res = maybe (BndErr undefined) (either BndErr BndRes) (zRes z)
          in return (fin res,res)

-- | Return Just when we have a result the could be the restult of the
-- poperator. This decides when to stop working.
boolEvolutionControl
  :: forall op m p .
  (Ord (BoolBound op),Monad m,BoolOp op)
  => GConf (BoolTag op p)
  -> Zipper (BoolTag op p) (ArrProc (BoolTag op p) m)
  -> Maybe (BndR (BoolTag op p))
boolEvolutionControl conf z = case confCap conf of
  ForceResult -> zRes z >>= \case
    Left _e -> Nothing -- xxx: should check if zero is even possible.
    Right x -> if zFinished z then Just $ BndRes x else Nothing
  CapVal cap -> do
    localBnd <- zBound z
    if cap < localBnd then return $ BndBnd localBnd else Nothing

zLocalIsFinal
  :: BoolOp op => Zipper (BoolTag op p) (ArrProc (BoolTag op p) m) -> Bool
zLocalIsFinal z = zIsAbsorbing z || (zEmptyInits z && zEmptyIts z)
zEmptyInits :: Zipper (BoolTag op p) (ArrProc (BoolTag op p) m) -> Bool
zEmptyInits = null . bgsInits . zBgState
zEmptyIts :: Zipper (BoolTag op p) (ArrProc (BoolTag op p) m) -> Bool
zEmptyIts = isNothing . acNonEmpty . bgsIts . zBgState
zIsAbsorbing
  :: BoolOp op => Zipper (BoolTag op p) (ArrProc (BoolTag op p) m) -> Bool
zIsAbsorbing z = case zRes z of
  Just (Right r) -> case elemType r of
    AbsorbingElem -> True
    NormalElem    -> False
  _ -> False

-- | The problem is that there is no w type in Conf w, just ZCap w so
-- we need to translate Conf w into a functor of ZCap w. This can be
-- done via generics. Then we can coerce without any problems.
convBool :: Monad m => ArrProc (BoolTag op p) m -> ArrProc (BoolTag op' p) m
convBool = dimap (to . coerce . from) (to . coerce . from)
notBool :: Monad m => ArrProc (BoolTag op p) m -> ArrProc (BoolTag op p) m
notBool = rmap $ \case
  BndRes r -> BndRes $ flipB r
  BndBnd r -> BndBnd $ flipB r
  BndErr e -> BndErr e
  where
    flipB GBool {..} = GBool { gbTrue = gbFalse,gbFalse = gbTrue }

data AnyOp
-- | Test
data BExp
  = BExp :/\: BExp
  | BExp :\/: BExp
  | BNot BExp
  | BEVar IM.Key -- Expression reference
  | BLVar IM.Key -- Variable reference

-- | Each constructor is a mech except for Let. Closures are not
-- trivial to implement here
interpretBExp
  :: forall m p .
  (MonadState (ProcMap (BoolTag AnyOp p) m) m
  ,MonadReader (IS.IntSet,IM.IntMap Bool) m
  ,AShow (ExtCoEpoch p)
  ,ExtParams p)
  => BExp
  -> ArrProc (BoolTag AnyOp p) m
interpretBExp = recur
  where
    recur :: BExp -> ArrProc (BoolTag AnyOp p) m
    recur = \case
      e :/\: e' -> convAnd $ mkProc $ convBool . recur <$> [e,e']
      e :\/: e' -> convOr $ mkProc $ convBool . recur <$> [e,e']
      BNot e -> notBool $ recur e
      BLVar k -> mealyLift $ fromKleisli $ const $ asks $ \(_trail,m) -> maybe
        (BndErr $ error $ "Failed to dereference value key: " ++ ashow (k,m))
        (BndRes . fromBool)
        $ IM.lookup k m
      BEVar k -> handleCycles k
        $ getUpdMech
          (BndErr $ error $ "Failed to dereference expr key: " ++ show k)
          k
    handleCycles k = withTrail $ \(trail,vals) -> if k
      `IS.member` trail then Left ErrCycle { ecCur = k,ecPred = mempty }
      else Right (IS.insert k trail,vals)
    fromBool c = GBool { gbTrue = Exists c,gbFalse = Exists $ not c }
    convOr :: ArrProc (BoolTag Or p) m -> ArrProc (BoolTag op p) m
    convOr = convBool
    convAnd :: ArrProc (BoolTag And p) m -> ArrProc (BoolTag op p) m
    convAnd = convBool




-- TOWRITE:
-- AND and OR are both absorbing/identity groups
-- Boolean expressions can be summarized as
-- * the expression
-- * The zipper (ie the expression + the state of evaluation)
-- * A pair of bools (absorbing, non-absorbing)
-- * A pair of bools & the operation to combine them (true false)
-- We use all three in different contexts.
andToOrConv :: Conv (BoolTag And m) (BoolTag Or m)
andToOrConv = coerceConv GenericConv
orToAndConv :: Conv (BoolTag Or m) (BoolTag And m)
orToAndConv = coerceConv GenericConv
