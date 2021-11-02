module FluiDB.Schema.Common (annotateQuery) where

import           Control.Monad.State
import           Data.Bifunctor
import           Data.Bitraversable
import           Data.Codegen.Build
import           Data.Codegen.Schema
import qualified Data.HashSet                         as HS
import           Data.QnfQuery.Build
import           Data.Query.Algebra
import           Data.Query.Optimizations.RemapUnique
import           Data.Query.QuerySchema
import           Data.Query.QuerySize
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.Unsafe
import           FluiDB.Types

annotateQuery
  :: Hashables2 e s
  => QueryCppConf e s
  -> (s -> Maybe TableSize)
  -> Query e s
  -> Either (GlobalError e s t n) (Query (ShapeSym e s) (QueryShape e s,s))
annotateQuery cppConf luSize q = do
  qUniqRemaped :: Query e s <- maybe
    (throwAStr "Couldn't find uniq:")
    ((>>= maybe (throwAStr "Couldn't remape uniques") (return . psQuery))
     . headListT
     . (`evalStateT` (0 :: Int))
     . remapUnique)
    $ traverse (mkPrelimShape cppConf) q
  let mkShape s = do
        size <- luSize s
        mkShapeFromTbl cppConf size s
  first toGlobalError
    $ (>>= maybe (throwAStr "Unknown symbol") return)
    $ fmap
      (bitraverse (pure . uncurry mkShapeSym) (\s -> (,s) <$> mkShape s)
       . snd
       . fromJustErr)
    $ (`evalStateT` def)
    $ headListT
    $ toQNF (fmap2 snd . tableSchema cppConf) qUniqRemaped


mkPrelimShape :: Hashables1 e => QueryCppConf e s -> s -> Maybe (PrelimShape e s)
mkPrelimShape cppConf s = do
  uniq <- HS.fromList <$> uniqueColumns cppConf s
  sch <- tableSchema cppConf s
  return
    $ PrelimShape { psQuery = Q0 s,psUniq = [uniq],psExtent = snd <$> sch }
