{-# LANGUAGE DeriveFoldable    #-}
{-# LANGUAGE DeriveFunctor     #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE DeriveTraversable #-}

module Data.CppAst.Template
  ( Template(..)
  ) where
import           Data.CppAst.Class
import           Data.CppAst.Function
import           Data.CppAst.TmplDefArg
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics           (Generic)

data Template a = TemplateClass [TmplDefArg a] (Class a)
                | TemplateFunction [TmplDefArg a] (Function a)
  deriving (Eq, Show, Functor, Foldable, Traversable, Generic)
instance Hashable a => Hashable (Template a)
instance AShow a => AShow (Template a)
