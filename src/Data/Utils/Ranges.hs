module Data.Utils.Ranges (fullRange, fullRange2) where

fullRange :: (Enum a, Bounded a) => [a]
fullRange = [minBound  .. maxBound]
fullRange2 :: (Enum a, Bounded a, Enum b, Bounded b) => [(a, b)]
fullRange2 = (,) <$> fullRange <*> fullRange
