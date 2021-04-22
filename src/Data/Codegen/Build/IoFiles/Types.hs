{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase    #-}

module Data.Codegen.Build.IoFiles.Types
  ( NodeRole(..)
  , MaybeBuild
  , IOFilesGF
  , IOFilesG
  , IOFiles
  ) where

import Data.Cluster.Types.Clusters
import           Data.Bifoldable
import           Data.Bifunctor
import           Data.Bitraversable
import           Data.Functor.Identity
import           Data.Utils.Hashable
import           Data.Void
import           Data.Codegen.Build.Types
import           Data.Query.SQL.FileSet
import           Data.NodeContainers
import           GHC.Generics

-- | Cluster extensions
data NodeRole interm i o = Input i
                         | Output o
                         | Intermediate interm
  deriving (Eq, Show, Generic)
instance Hashables3 a b c => Hashable (NodeRole a b c)
instance Bifunctor (NodeRole x) where
  bimap f g = \case
    Input x -> Input $ f x
    Output x -> Output $ g x
    Intermediate a -> Intermediate a

instance Bitraversable (NodeRole x) where
  bitraverse f g =  \case
    Input x -> Input <$> f x
    Output x -> Output <$> g x
    Intermediate a -> pure $ Intermediate a

instance Bifoldable (NodeRole x) where
  bifoldr f g x0 =  \case
    Input x -> f x x0
    Output x -> g x x0
    _ -> x0

-- | The input/output files of an operation.
type IOFiles e s =
  IOFilesG (NodeRef ()) (Maybe (QueryPlan e s),FilePath) (Maybe (QueryPlan e s),FileSet)
-- | If a file has Nothing in the filename, it means we should not be
-- making it.
type IOFilesG interm inp out = IOFilesGF MaybeBuild interm inp out
type IOFilesGF f interm inp out =
  AnyCluster' Void Identity ()
  (NodeRole interm (f inp) (f out))
type MaybeBuild = Maybe
