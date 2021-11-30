module Data.Query.QuerySchema
  (querySchema
  ,isUniqueSel
  ,shapeSymEqs
  ,joinShapes
  ,shapeProject
  ,refersToShape
  ,shapeSymOrig
  ,mkShapeSym
  ,setShapeSymOrig
  ,mkSymShapeSymNM
  ,shapeSymIsSym
  ,shapeSymTypeSym'
  ,getQueryShapePrj
  ,getQueryShapeGrp
  ,exprColumnProps
  ,shapeAllSyms
  ,complementProj
  ,exprCppType
  ,mkLitShapeSym
  ,shapeSymType
  ,mkQueryShape
  ,getSymShape
  ,schemaQP
  ,translateShape'
  ,translateShapeMap''
  ,lookupQP
  ,ShapeSym(..)
  ,QueryShape'(..)
  ,QueryShape
  ,ColumnProps(..)) where


import           Data.Query.QuerySchema.GetQueryShape
import           Data.Query.QuerySchema.SchemaBase
import           Data.Query.QuerySchema.Types
