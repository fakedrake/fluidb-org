module Data.Query.Optimizations.Echo
  (TQuery(..)
  ,EchoId
  ,EchoNetEden(..)
  ,EchoSide(..)
  ,tunnelQuery) where
import           Control.Monad
import           Data.Bifunctor
import qualified Data.IntMap        as IM
import qualified Data.IntSet        as IS
import           Data.Query.Algebra
import           Data.Utils.Default

-- | A tunnel network that has not yet been
newtype EchoNetEden = EchoNetEden { unEchoNetEden :: IM.IntMap IS.IntSet}
type EchoId = IM.Key
data EchoSide = TEntry EchoId | TExit EchoId | NoEcho
instance Default EchoSide where
  def = NoEcho
-- | Echo marked query
data TQuery e s =
  TQuery { tqQuery :: Either (Query e (TQuery e s)) (Query e s)
          ,tqEcho  :: EchoSide
         }

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
