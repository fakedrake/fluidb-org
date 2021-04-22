{-# LANGUAGE DeriveFoldable      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Data.CppAst.LiteralCode.CodeCache
  ( CodeCache
  , normalizeCode
  , runCodeCache
  , sortDefs
  ) where

import           Data.CppAst.CodeSymbol
import           Data.Foldable
import qualified Data.HashMap.Strict    as HM
import qualified Data.HashSet           as HS
import qualified Data.IntMap            as IM
import           Data.List
import           Data.Maybe
import           Data.Utils.Hashable
import           GHC.Generics
type CodeCache t =
  HM.HashMap
    (MinimalIndexes (t (Either CodeSymbol CodeSymbol)))
    (t CodeSymbol)

newtype MinimalIndexes x =
  MkMinimalIndexes { runMinimalIndexes :: x
                   }
  deriving (Eq,Foldable,Generic)

instance Hashable x => Hashable (MinimalIndexes x)

-- | Return a traversable with the same structure but such that the
-- indexes defined within the structure are minimal. Either is to
-- differentiate between internal references and external ones. Left
-- means external reference, Right means internal reference for ref
-- symbols. All def symbols are Right.
normalizeCode :: (Foldable t, Traversable t) =>
                 t CodeSymbol
               -> Maybe (MinimalIndexes (t (Either CodeSymbol CodeSymbol)))
normalizeCode cls = let
  defList = catMaybes $ toList $ getDef <$> cls
  defMap = IM.fromList $ zip defList [0..]
  getDef = \case
    UniqueSymbolDef _ i -> Just i
    _ -> Nothing
  -- Change the symbol refs tha refer to internal defs to the new def
  -- number. (There may be collisions with external refs).
  luRef = \case
    UniqueSymbolRef s i -> Just $ fromMaybe (Left $ UniqueSymbolRef s i)
                          $ Right . UniqueSymbolRef s
                          <$> i `IM.lookup` defMap
    UniqueSymbolDef s i ->  Right . UniqueSymbolDef s <$> i `IM.lookup` defMap
    x@(CppSymbol _) -> Just $ Right x
    x@(CppLiteralSymbol _) -> Just $ Right x
  in if length defList == length defMap
     then MkMinimalIndexes <$> traverse luRef cls
     else Nothing -- There is a duplicate definition index!

runCodeCache :: CodeCache t -> [t CodeSymbol]
runCodeCache = toList

-- | Topological sort of definitions. If there is a circular
-- definition return the circle.
--
-- All classes refer to their own symbol so we disallow 1 step cycles.
sortDefs :: forall t .
           (Hashable (t CodeSymbol),
            Eq (t CodeSymbol), Traversable t) =>
           (t CodeSymbol -> CodeSymbol)
         -> [t CodeSymbol]
         -> Either [t CodeSymbol] [t CodeSymbol]
sortDefs getRef objs = go refSetInit objs where
  refSetInit = HS.fromList $ getRef <$> objs
  go :: HS.HashSet CodeSymbol -> [t CodeSymbol] -> Either [t CodeSymbol] [t CodeSymbol]
  go _ [] = Right []
  go refSet cands = case noRefs of
    [] -> Left cands -- There is a cycle somewhere in cands.
    _  -> (noRefs ++) <$> go newRefSet newCand
    where
      newRefSet = foldr (HS.delete . getRef) refSet noRefs
      (newCand, noRefs) = partition (\c -> any (isRef $ getRef c) c) cands
      isRef self ref = ref /= self && ref `HS.member` refSet
