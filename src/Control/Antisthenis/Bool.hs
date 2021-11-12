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
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}
module Control.Antisthenis.Bool
  (BoolV(..)
  ,BoolOp(..)
  ,GBool(..)
  ,BoolCap
  ,BoolBound
  ,And
  ,Or
  ,BoolTag
  ,convBool) where

import           Control.Antisthenis.AssocContainer
import           Control.Antisthenis.Types
import           Control.Monad
import           Control.Utils.Free
import           Data.Bifunctor
import           Data.Coerce
import           Data.Maybe
import           Data.Profunctor
import           Data.Proxy
import           Data.Utils.AShow
import           Data.Utils.Const
import           Data.Utils.Debug
import           Data.Utils.Heaps
import           Data.Utils.Unsafe
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
instance Ord Cost where
  compare (Cost a) (Cost b) = compare a b
instance Semigroup Cost where
  Cost a <> Cost b = Cost $ a + b
instance Monoid Cost where
  mempty = Cost 0

-- | Absorbing is used for caps.
data GAbsorbing v = GAbsorbing { gaAbsorbing :: v,gaNonAbsorbing :: v }

-- | Bool is used for bounds
data GBool op v = GBool { gbTrue :: v,gbFalse :: v }
  deriving (Eq,Generic)
instance AShow v => AShow  (GBool op v)
type BoolCap op = GBool op (Maybe Cost)
type BoolBound op = GBool op Cost
newtype BoolV op = BoolV Bool deriving (Generic,Show,Eq)
instance AShow (BoolV Or) where
  ashow' (BoolV bv) = sexp "BoolV" [ashow' bv,Sym "::",Sym "Or"]
instance AShow (BoolV And) where
  ashow' (BoolV bv) = sexp "BoolV" [ashow' bv,Sym "::",Sym "And"]

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

-- | Bounds are
instance Ord (BoolBound op) where
  compare (GBool a b) (GBool a' b') =
    compare (min a b) (min a' b')

data Or
data And

-- | Bet the bound of the current
--
-- non-absorbings  = sum the non-absorbing iterations
-- absorbings  = min of the absorbing iterations
zBound :: BoolOp op
       => Zipper (BoolTag op p) (ArrProc (BoolTag op p) m)
       -> Maybe (BoolBound op)
zBound z = do
  absorb <- fmap toGAbsorbing $ chMinKey $ bgsIts bgs
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
zFinished
  :: BoolOp op => Zipper (BoolTag op p) (ArrProc (BoolTag op p) m) -> Bool
zFinished z =
  trace
    ("Bool finished?"
     <: (first (const ()) <$> zRes z
        ,length $ bgsInits $ zBgState z
        ,void $ acNonEmpty $ bgsIts $ zBgState z
        ,ret))
    ret
  where
    ret = isAbsorb || emptyIniIt (zBgState z)
    emptyIniIt ZipState {..} = null bgsInits && isNothing (acNonEmpty bgsIts)
    isAbsorb = case zRes z of
      Just (Right x) -> case elemType x of
        AbsorbingElem -> True
        NormalElem    -> False
      _ -> False

data ElemType = AbsorbingElem | NormalElem
class (AShow (BoolV op),Semigroup (BoolV op)) => BoolOp op where
  -- | Whether the Bool or error is trumped or trumps the result
  -- depends on the operation.
  toGAbsorbing :: GBool op v -> GAbsorbing v
  toGBool :: GAbsorbing v -> GBool op v
  elemType :: BoolV op -> ElemType

instance Semigroup (BoolV Or) where
  BoolV a <> BoolV b = BoolV $ a || b

instance Semigroup (BoolV And) where
  BoolV a <> BoolV b = BoolV $ a && b

instance BoolOp Or where
  toGAbsorbing
    GBool {..} = GAbsorbing { gaNonAbsorbing = gbFalse,gaAbsorbing = gbTrue }
  {-# INLINE toGAbsorbing #-}
  toGBool
    GAbsorbing {..} = GBool { gbFalse = gaNonAbsorbing,gbTrue = gaAbsorbing }
  {-# INLINE toGBool #-}
  elemType (BoolV x) = if x then AbsorbingElem else NormalElem


instance BoolOp And where
  toGAbsorbing
    GBool {..} = GAbsorbing { gaNonAbsorbing = gbTrue,gaAbsorbing = gbFalse }
  {-# INLINE toGAbsorbing #-}
  toGBool
    GAbsorbing {..} = GBool { gbTrue = gaNonAbsorbing,gbFalse = gaAbsorbing }
  {-# INLINE toGBool #-}
  elemType (BoolV x) = if x then  NormalElem else AbsorbingElem


-- assocMinCost
--   :: BoolOp op
--   => CountingAssoc Maybe (BoolBound op) v
--   -> Maybe (GAbsorbing Cost)
-- assocMinCost assoc = case toGAbsorbing . fst <$> l of
--   []   -> Nothing
--   x:xs -> Just $ foldl' (<>) x xs
--   where
--     SimpleAssoc l = assocData assoc

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

data BoolTag op p
instance ExtParams (BoolTag op p) p => BndRParams (BoolTag op p) where
  type ZErr (BoolTag op p) = ExtError p
  type ZBnd (BoolTag op p) = BoolBound op
  type ZRes (BoolTag op p) = BoolV op
  -- | Select the best case of the optimistic of two. We declare a
  -- lower (better) bound to be the most optimistic of the two. But
  -- when comparing with the cap the logic is different
  bndLt Proxy b1 b2 = b1 < b2
  exceedsCap Proxy cap bnd =
    gbTrue cap .< gbTrue bnd || gbFalse cap .< gbFalse bnd
    where
      Just a .< b  = a < b
      Nothing .< _ = False

instance (AShow (ExtCoEpoch p),ExtParams (BoolTag op p) p,BoolOp op)
  => ZipperParams (BoolTag op p) where
  type ZEpoch (BoolTag op p) = ExtEpoch p
  type ZCoEpoch (BoolTag op p) = ExtCoEpoch p
  type ZCap (BoolTag op p) = GBool op (Maybe Cost)
  type ZPartialRes (BoolTag op p) =
    Maybe (Either (ZErr (BoolTag op p)) (ZRes (BoolTag op p)))
  type ZItAssoc (BoolTag op p) =
    CHeap (ZBnd (BoolTag op p))
  zEvolutionControl = boolEvolutionControl
  zEvolutionStrategy = boolEvolutionStrategy
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
      bnd <- chMinKey $ bgsIts $ zBgState z
      gcap <- case confCap conf of
        CapVal cap -> Just cap
        _          -> Nothing
      return $ minBndCap bnd gcap }

minBndCap :: GBool op Cost -> GBool op (Maybe Cost) -> GBool op (Maybe Cost)
minBndCap bnd cap =
  GBool { gbTrue = Just $ min' (gbTrue cap) (gbTrue bnd)
         ,gbFalse = Just $ min' (gbFalse cap) (gbFalse bnd)
        }
  where
    min' Nothing x  = x
    min' (Just x) y = min x y

-- boolEvolutionStrategy
--   :: Monad m
--   => (BndR (BoolTag op p) -> x)
--   -> FreeT
--     (ItInit
--        (ExZipper (BoolTag op p))
--        (CountingAssoc Maybe (ZBnd (BoolTag op p))))
--     m
--     (x,BndR (BoolTag op p))
--   -> m (x,BndR (BoolTag op p))

-- | Remember the cursor is the result.
boolEvolutionStrategy
  :: Monad m
  => FreeT (Cmds (BoolTag op p)) m x
  -> m
    (Maybe (ResetCmd (FreeT (Cmds (BoolTag op p)) m x))
    ,Either (ExtCoEpoch p,BndR (BoolTag op p)) x)
boolEvolutionStrategy = recur Nothing
  where
    recur rst (FreeT m) = m >>= \case
      Pure a -> return (rst,Right a)
      Free cmd -> case cmdItCoit cmd of
        CmdItInit _it ini -> recur (Just $ cmdReset cmd) ini
        CmdIt it -> recur (Just $ cmdReset cmd)
          $ it
          $ (\((k,v),h) -> (k,v,h)) . fromJustErr . popHeap . runNonEmptyF
        CmdInit ini -> recur (Just $ cmdReset cmd) ini
        CmdFinished (ExZipper z) -> return
          (rst
          ,Left
             (getConst $ zCursor z
             ,maybe (BndErr undefined) (either BndErr BndRes) (zRes z)))

-- | Return Just when we have a result the could be the restult of the
-- poperator. This decides when to stop working.
boolEvolutionControl
  :: forall op m p .
  (BoolOp op,ExtParams (BoolTag op p) p)
  => GConf (BoolTag op p)
  -> Zipper (BoolTag op p) (ArrProc (BoolTag op p) m)
  -> Maybe (BndR (BoolTag op p))
boolEvolutionControl conf z = case confCap conf of
  ForceResult -> zRes z >>= \case
    Left _e -> Nothing -- xxx: should check if zero is even possible.
    Right x -> if zFinished z then Just $ BndRes x else Nothing
  CapVal cap -> do
    localBnd <- zBound z
    if exceedsCap @(BoolTag op p) Proxy cap localBnd
      then return $ BndBnd localBnd else Nothing

-- | The problem is that there is no w type in Conf w, just ZCap w so
-- we need to translate Conf w into a functor of ZCap w. This can be
-- done via generics. Then we can coerce without any problems.
convBool :: Monad m => ArrProc (BoolTag op p) m -> ArrProc (BoolTag op' p) m
convBool = dimap (to . coerce . from) (to . coerce . from)

-- notBool :: Monad m => ArrProc (BoolTag op p) m -> ArrProc (BoolTag op p) m
-- notBool = rmap $ \case
--   BndRes r -> BndRes $ flipB r
--   BndBnd r -> BndBnd $ flipB r
--   BndErr e -> BndErr e
--   where
--     flipB GBool {..} = GBool { gbTrue = gbFalse,gbFalse = gbTrue }

-- data AnyOp
-- | Test
-- data BExp
--   = BExp :/\: BExp
--   | BExp :\/: BExp
--   | BNot BExp
--   | BEVar IM.Key -- Expression reference
--   | BLVar IM.Key -- Variable reference

-- -- | Each constructor is a mech except for Let. Closures are not
-- -- trivial to implement here
-- interpretBExp
--   :: forall m p .
--   (MonadState (ProcMap (BoolTag AnyOp p) m) m
--   ,MonadReader (IS.IntSet,IM.IntMap Bool) m
--   ,Eq (ZCoEpoch (BoolTag AnyOp p))
--   ,AShow (ExtCoEpoch p)
--   ,AShow (ExtEpoch p)
--   ,ExtError p ~ IndexErr IS.Key
--   ,ExtParams (BoolTag Or p) p
--   ,ExtParams (BoolTag And p) p)
--   => BExp
--   -> ArrProc (BoolTag AnyOp p) m
-- interpretBExp = recur
--   where
--     recur :: BExp -> ArrProc (BoolTag AnyOp p) m
--     recur = \case
--       e :/\: e' ->
--         convAnd $ mkProcId (zidDefault "and") $ convBool . recur <$> [e,e']
--       e :\/: e' ->
--         convOr $ mkProcId (zidDefault "or") $ convBool . recur <$> [e,e']
--       BNot e -> notBool $ recur e
--       BLVar k -> mealyLift $ fromKleisli $ const $ asks $ \(_trail,m) -> maybe
--         (BndErr $ error $ "Failed to dereference value key: " ++ ashow (k,m))
--         (BndRes . fromBool)
--         $ IM.lookup k m
--       BEVar k -> handleCycles k
--         $ getUpdMech
--           (BndErr $ error $ "Failed to dereference expr key: " ++ show k)
--           k
--     handleCycles k = withTrail $ \(trail,vals) ->
--       if k `IS.member` trail then Left ErrCycle { ecCur = k,ecPred = mempty }
--       else Right (IS.insert k trail,vals)
--     fromBool c = GBool { gbTrue = Exists c,gbFalse = Exists $ not c }
--     convOr :: ArrProc (BoolTag Or p) m -> ArrProc (BoolTag op p) m
--     convOr = convBool
--     convAnd :: ArrProc (BoolTag And p) m -> ArrProc (BoolTag op p) m
--     convAnd = convBool

-- towrite:
-- AND and OR are both absorbing/identity groups
-- Boolean expressions can be summarized as
-- * the expression
-- * The zipper (ie the expression + the state of evaluation)
-- * A pair of bools (absorbing, non-absorbing)
-- * A pair of bools & the operation to combine them (true false)
-- We use all three in different contexts.
