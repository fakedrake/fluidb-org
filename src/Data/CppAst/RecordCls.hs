{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
module Data.CppAst.RecordCls (recordCls) where

import           Data.Bifunctor
import           Data.CppAst.Argument
import           Data.CppAst.Class
import           Data.CppAst.CodeSymbol
import           Data.CppAst.Constructor
import           Data.CppAst.CppType
import           Data.CppAst.Declaration
import           Data.CppAst.Expression
import           Data.CppAst.Function
import           Data.CppAst.Operator
import           Data.CppAst.Statement
import           Data.CppAst.Symbol
import           Data.CppAst.TypeModifier
import           Data.List
import qualified Data.Set                 as DS
import           Data.Tuple
import           Data.Utils.Function


recordCls :: Symbol CodeSymbol -> [(CppType, CodeSymbol)] -> Class CodeSymbol
recordCls name sch =
  Class
  { className = name
   ,classConstructors =
      [Constructor
         { constructorBody = []
          ,constructorMemberConstructors = constructorCalls
          ,constructorArguments = constructorArgs
         }
       -- Empty rectord
      ,Constructor
         { constructorBody = []
          ,constructorMemberConstructors = []
          ,constructorArguments = []
         }]
   ,classPublicFunctions =
      [showRecord
      ,overloadOperator "&&" "<"
      ,overloadOperator "&&" "=="
      ,overloadOperator "||" "!="]
   ,classPublicMembers = memberDecls
   ,classTypeDefs = []
   ,classPrivateMembers = []
  }
  where
    nameRef = fmap symbolRef name
    memberDecls = schemaDecls id
    underscoreSym (Symbol sym) = Symbol $ case sym of
      UniqueSymbolDef x i -> UniqueSymbolRef ("_" <> x) i
      UniqueSymbolRef x i -> UniqueSymbolRef ("_" <> x) i
      CppSymbol x         -> CppSymbol $ "_" <> x
      CppLiteralSymbol x  -> CppSymbol $ "_" <> x
    constructorArgs = Argument <$> schemaDecls underscoreSym
    mkConstructorCall x = (x,SymbolExpression $ underscoreSym x)
    constructorCalls = mkConstructorCall . declarationNameRef <$> memberDecls
    overloadOperator logicalComb op =
      Function
      { functionName = Symbol $ CppLiteralSymbol $ "operator " ++ op
       ,functionType = PrimitiveType mempty CppBool
       ,functionArguments =
          let recSym = Symbol $ CppSymbol "otherRec"
          in [Argument
                Declaration
                { declarationName = recSym
                 ,declarationType =
                    ClassType (DS.fromList [CppConst,Reference]) [] nameRef
                }]
       ,functionConstMember = True
       ,functionBody =
          [ReturnSt $ foldr1 bin $ expr1 . declarationNameRef <$> memberDecls]
      }
      where
        bin :: Expression CodeSymbol
            -> Expression CodeSymbol
            -> Expression CodeSymbol
        bin = Parens ... E2ession (Operator logicalComb)
        expr1 :: Symbol CodeSymbol -> Expression CodeSymbol
        expr1 s =
          E2ession
            (Operator op)
            (ObjectMember (SymbolExpression (Symbol $ CppSymbol "otherRec")) s)
            (SymbolExpression s)
    showRecord =
      Function
      { functionName = "show"
       ,functionType = ClassType mempty [] "std::string"
       ,functionArguments = []
       ,functionConstMember = True
       ,functionBody =
          [DeclarationSt
             Declaration
             { declarationName = "o"
              ,declarationType = "std::stringstream"
             }
             Nothing
          ,StreamSt "o"
             $ intersperse (LiteralStringExpression " | ")
             $ wrapArr <$> memberDecls
          ,ReturnSt $ FunctionAp (InstanceMember "o" "str") [] []]
      }
    wrapArr d@Declaration {..} = case declarationType of
      PrimitiveType _ (CppArray CppChar _) -> FunctionAp "arrToString" []
        $ return
        $ SymbolExpression
        $ declarationNameRef d
      _ -> SymbolExpression $ declarationNameRef d
    schemaDecls
      :: (Symbol CodeSymbol -> Symbol CodeSymbol) -> [Declaration CodeSymbol]
    schemaDecls f =
      uncurry Declaration . ((f . Symbol) `bimap` PrimitiveType mempty) . swap
      <$> sch
