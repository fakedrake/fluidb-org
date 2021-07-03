{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase    #-}

module Data.QnfQuery.HashBag
  (HashBag(..)
  , bagMember
  , bagInsert
  , bagDelete
  , bagMap
  , bagToList
  , bagSingleton
  , toHashBag
  , isSubbagOf
  , bagTraverse
  , assocToHashBag
  , bagMultiplicity
  , bagNull
  , bagToAssoc
  , bagIntersects
  ) where

import           Data.Bitraversable
import qualified Data.HashMap.Strict as HM
import           Data.Maybe
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Hashable
import           GHC.Generics

newtype HashBag a = HashBag {unHashBag :: HM.HashMap a Int}
  deriving (Show, Read, Generic, Eq)
instance Hashable a => Hashable (HashBag a)
instance AShow a => AShow (HashBag a) where
  ashow' (HashBag hs) = sexp "assocToHashBag" [ashow' $ HM.toList hs]
instance (Hashables1 a, ARead a) => ARead (HashBag a) where
  aread' = \case
    Sub [Sym "assocToHashBag", x] -> assocToHashBag <$> aread' x
    _ -> Nothing
instance Foldable HashBag where
  foldr f ini = foldr f ini . bagToList
instance Hashables1 a => Monoid (HashBag a) where
  mempty = HashBag mempty
instance Hashables1 a => Semigroup (HashBag a) where
  a <> b = HashBag $ HM.unionWith (+) (unHashBag a) (unHashBag b)
instance Hashables1 a => Default (HashBag a) where def = mempty
bagToAssoc :: HashBag a -> [(a,Int)]
bagToAssoc = HM.toList . unHashBag
bagToList :: HashBag a -> [a]
bagToList = mconcat . fmap (uncurry $ flip replicate) . bagToAssoc
bagNull :: HashBag a -> Bool
bagNull = HM.null . unHashBag
bagMember :: Hashables1 a => a -> HashBag a -> Bool
bagMember a = HM.member a . unHashBag
bagInsert :: Hashables1 a => a -> HashBag a -> HashBag a
bagInsert a = HashBag . HM.alter (Just . maybe 1 (+1)) a . unHashBag
bagDelete :: Hashables1 a => a -> HashBag a -> HashBag a
bagDelete a = HashBag
            . HM.alter (>>= \case {0 -> Nothing; x -> Just $ x+1}) a
            . unHashBag
bagMap :: Hashables1 a' => (a -> a') -> HashBag a -> HashBag a'
bagTraverse :: (Applicative f, Hashables1 a') =>
              (a -> f a') -> HashBag a -> f (HashBag a')
bagTraverse f = fmap (HashBag . HM.fromList)
                . traverse (bitraverse f pure)
                . HM.toList
                . unHashBag
bagMap f = HashBag . foldr ins mempty . HM.toList . unHashBag where
  ins (v,i) = HM.alter (Just . maybe i (+i)) $ f v
bagSingleton :: Hashables1 a => a -> HashBag a
bagSingleton = HashBag . (`HM.singleton` 1)
toHashBag :: (Foldable f, Hashables1 a) => f a -> HashBag a
toHashBag = foldl (flip bagInsert) mempty
assocToHashBag :: Hashables1 a => [(a,Int)] -> HashBag a
assocToHashBag = HashBag . HM.fromList
isSubbagOf :: Hashables1 a => HashBag a -> HashBag a -> Bool
isSubbagOf a b = all go $ HM.toList $ unHashBag a where
  go (ai,cnt) = maybe False (>= cnt) $ HM.lookup ai $ unHashBag b
bagMultiplicity :: Hashables1 a => a -> HashBag a -> Int
bagMultiplicity a = fromMaybe 0 . HM.lookup a . unHashBag
bagIntersects :: Hashables1 a => HashBag a -> HashBag a -> Bool
bagIntersects hb hb' = not $ HM.null $ HM.intersection (unHashBag hb) (unHashBag hb')
