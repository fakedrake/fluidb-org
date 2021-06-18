module FluiDB.Bamify.Common (schemaPostPaddings) where

schemaPostPaddings :: CppSchema -> Maybe [Int]
schemaPostPaddings [] = Just []
schemaPostPaddings [_] = Just [0]
schemaPostPaddings schema = do
  elemSizes <- sequenceA [cppTypeSize t | (t,_) <- schema]
  spaceAligns' <- sequenceA [cppTypeAlignment t | (t,_) <- schema]
  let (_:spaceAligns) = spaceAligns' ++ [maximum spaceAligns']
  let offsets = 0 : zipWith3 getDataOffset spaceAligns offsets elemSizes
  return $ zipWith (-) (zipWith (-) (tail offsets) offsets) elemSizes
  where
    getDataOffset nextAlig off size =
      (size + off)
      + ((nextAlig - ((size + off) `mod` nextAlig)) `mod` nextAlig)
