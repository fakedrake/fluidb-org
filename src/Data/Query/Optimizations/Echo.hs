module Data.Query.Optimizations.Echo
  (TQuery(..)
  ,EchoId
  ,EchoNetEden(..)
  ,EchoSide(..)
  ,tunnelQuery) where
import           Control.Monad
import           Data.Bifunctor
import qualified Data.IntMap         as IM
import qualified Data.IntSet         as IS
import           Data.Query.Algebra
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Hashable
import           GHC.Generics

-- | A tunnel network that has not yet been
newtype EchoNetEden = EchoNetEden { unEchoNetEden :: IM.IntMap IS.IntSet}
type EchoId = IM.Key
data EchoSide
  = TEntry EchoId
  | TExit EchoId
  | NoEcho
  deriving (Generic,Eq)
instance Default EchoSide where
  def = NoEcho
instance AShow EchoSide
-- | Echo marked query
data TQuery e s =
  TQuery { tqQuery :: Either (Query e (TQuery e s)) (Query e s)
          ,tqEcho  :: EchoSide
         }
  deriving (Generic,Eq)

instance (AShow s,AShow e) => AShow (TQuery e s)
instance Foldable (TQuery s) where
  foldr f ini TQuery{tqQuery=q0} = case q0 of
    Left q  -> foldr (flip $ foldr f) ini q
    Right q -> foldr f ini q

instance Bifunctor TQuery where
  bimap f g = go
    where
      go TQuery {..} =
        TQuery
        { tqQuery = bimap (bimap f go) (bimap f g) tqQuery,tqEcho = tqEcho }
instance Hashables2 e s => Hashable (TQuery e s)
instance Hashable EchoSide

instance Functor (TQuery e) where
  fmap f = go where go tq  = tq { tqQuery = bimap (fmap go) (fmap f) $ tqQuery tq}
  {-# INLINE fmap #-}
instance Monad (TQuery e) where
  return a = TQuery { tqQuery = pure $ pure a,tqEcho = def }
  {-# INLINE return #-}
  a >>= f = a { tqQuery = Left $ either (fmap (>>= f)) (fmap f) $ tqQuery a}
  {-# INLINE (>>=) #-}
instance Applicative (TQuery e) where
  pure = return
  {-# INLINE pure #-}
  f <*> x = ap f x
  {-# INLINE (<*>) #-}

tunnelQuery :: Query e s -> TQuery e s
tunnelQuery q = TQuery { tqQuery = Right q, tqEcho = def }
