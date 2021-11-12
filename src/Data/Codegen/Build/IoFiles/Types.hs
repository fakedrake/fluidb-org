{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase    #-}

module Data.Codegen.Build.IoFiles.Types
  (NodeRole(..)
  ,MaybeBuild
  ,IOFilesGF
  ,IOFilesG
  ,IOFiles
  ,IOFilesD(..),mapIntermRole,ashowIOFiles) where

import           Data.Bifoldable
import           Data.Bifunctor
import           Data.Bitraversable
import           Data.Cluster.Types.Clusters
import           Data.Codegen.Build.Types
import           Data.Functor.Identity
import           Data.NodeContainers
import           Data.Query.SQL.QFile
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           Data.Void
import           GHC.Generics

-- | Cluster extensions
data NodeRole interm i o
  = Input i
  | Output o
  | Intermediate interm
  deriving (Eq,Show,Generic)
instance (AShow interm,AShow i,AShow o)
  => AShow (NodeRole interm i o)
instance Hashables3 a b c => Hashable (NodeRole a b c)
instance Bifunctor (NodeRole x) where
  bimap f g = \case
    Input x        -> Input $ f x
    Output x       -> Output $ g x
    Intermediate a -> Intermediate a
mapIntermRole :: (int -> int') -> NodeRole int i o -> NodeRole int' i o
mapIntermRole f = \case
  Input a        -> Input a
  Output a       -> Output a
  Intermediate a -> Intermediate $ f a

instance Bitraversable (NodeRole x) where
  bitraverse f g =  \case
    Input x        -> Input <$> f x
    Output x       -> Output <$> g x
    Intermediate a -> pure $ Intermediate a

instance Bifoldable (NodeRole x) where
  bifoldr f g x0 =  \case
    Input x  -> f x x0
    Output x -> g x x0
    _        -> x0

-- | A cluster of io files paired with the direction of tirgger.
data IOFilesD e s = IOFilesD { iofDir :: Direction,iofCluster :: IOFiles e s }
-- | The input/output files of an operation.
type IOFiles e s =
  IOFilesG
    (NodeRef ())
    (Maybe (QueryShape e s),FilePath)
    (Maybe (QueryShape e s),QFile)

-- | If a file has Nothing in the filename, it means we should not be
-- making it.
type IOFilesG interm inp out = IOFilesGF MaybeBuild interm inp out
type IOFilesGF f interm inp out =
  AnyCluster' Void Identity ()
  (NodeRole interm (f inp) (f out))
type MaybeBuild = Maybe
ashowIOFiles :: AShow2 e s => IOFiles e s -> SExp
ashowIOFiles c =
  ashow'
    (ashow' <$> clusterInputs c
    ,ashow' . bimap (fmap snd) (fmap snd) . runIdentity <$> clusterOutputs c)
