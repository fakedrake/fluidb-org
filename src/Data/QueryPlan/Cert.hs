module Data.QueryPlan.Cert (Cert(..),incrTrail) where

import           Control.Applicative
import           Data.Pointed
import           Data.QueryPlan.Scalable
import           Data.Utils.AShow
import           Data.Utils.AShow.Print
import           Data.Utils.Nat
import           GHC.Generics

-- | The trailsize indicates "how many" (maximum depth) materialized
-- nodes were traversed to come up with this value. It is only useful
-- in conjuntion with the cap.
data Cert a = Cert { cTrailSize :: Int,cData :: a }
  deriving (Eq,Generic,Functor,Show)
instance Pointed Cert where point = Cert 0
instance Zero a => Zero (Cert a) where
  zero = point zero
  isNegative (Cert _ a) = isNegative a

instance AShow a => AShow (Cert a) where
  ashow' Cert {..} = Sym $ show cTrailSize ++ "m+" ++ ashowLine cData
-- There are no circumnstances under which we are going to chose a
-- value that has come throgh more materialied nodes. The scaling will
-- have damped it enough.
instance Ord a => Ord (Cert a) where
  compare (Cert _ a') (Cert _ b') = compare a' b'
instance Semigroup a => Semigroup (Cert a) where (<>) = liftA2 (<>)
instance Applicative Cert where
  pure = point
  Cert a f <*> Cert b x = Cert (max a b) $ f x
instance Scalable a => Scalable (Cert a) where scale sc = fmap $ scale sc
instance Subtr a => Subtr (Cert a) where subtr = liftA2 subtr
incrTrail :: Cert a -> Cert a
incrTrail c = c{cTrailSize=1 + cTrailSize c}
