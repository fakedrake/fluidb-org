module FluiDB.Schema.Common (annotateQuery) where

import           Control.Monad.State
import           Data.Bifunctor
import           Data.Bitraversable
import           Data.Codegen.Build
import           Data.Codegen.Schema
import           Data.QnfQuery.Build
import           Data.Query.Algebra
import           Data.Query.Optimizations.ExposeUnique
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
  let uniqSym = \e -> do
        i <- get
        case asUnique cppConf i e of
          Just e' -> modify (+ 1) >> return e'
          Nothing -> throwAStr $ "Not a symbol: " ++ ashow e
  qUniqExposed :: Query e s <- maybe
    (throwAStr "Couldn't find uniq:")
    ((>>= maybe (throwAStr "Couldn't expose uniques") (return . fst))
     . headListT
     . (`evalStateT` (0 :: Int))
     . exposeUnique uniqSym)
    $ traverse (\s -> (s,) <$> uniqueColumns cppConf s) q
  let mkShape s = do
        size <- luSize s
        mkShapeFromTbl cppConf size s
  first toGlobalError
    $ (>>= maybe (throwAStr "Unknown symbol") return)
    $ fmap
      (bitraverse
         (pure . uncurry mkShapeSym)
         (\s -> (,s) <$> mkShape s)
       . snd
       . fromJustErr)
    $ (`evalStateT` def)
    $ headListT
    $ toQNF (fmap2 snd . tableSchema cppConf) qUniqExposed
