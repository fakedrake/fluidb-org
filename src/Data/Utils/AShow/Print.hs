{-# LANGUAGE LambdaCase #-}

module Data.Utils.AShow.Print
  ( ashow
  , aprint
  , gshow
  , showSExpOneLine
  ) where

import           Data.List
import           Data.Utils.AShow.Common
import           Data.Utils.Functors
import           Text.Printf

-- |Show an s-expression without any newlines.
showSExpOneLine :: Bool -> SExp -> String
showSExpOneLine shouldShowParen = \case
  Rec name entries -> name ++ "{" ++ expShowRecs entries ++ "}"
  Str xs -> "\"" ++ xs ++ "\""
  Case entries -> "\\case {" ++ expShowCases entries ++ "}"
  Vec xs -> expShow False ("[","]") "," xs
  Tup xs -> expShow False ("(",")") "," xs
  Sub xs
    -> expShow True (if shouldShowParen then ("(",")") else ("","")) " " xs
  Sym x -> x
  where
    expShowCases entries = intercalate ";" $ entries <&> \(l,v)
      -> printf "%s->%s" l (showSExpOneLine False v)
    expShowRecs entries = intercalate "," $ entries <&> \(l,v)
      -> printf "%s=%s" l (showSExpOneLine False v)
    expShow :: Bool -> (String,String) -> String -> [SExp] -> String
    expShow propagate (b,e) s ls =
      b ++ intercalate s (showSExpOneLine propagate <$> ls) ++ e

-- | Trying 72 chars, give up on 200. XXX: omit the larger parts.
showSExp :: SExp -> String
showSExp firstSexp =
  if all (<= 200) $ length <$> lines res
  then res
  else showSExpOneLine False firstSexp
  where
    res = showSExpOff True 0 0 firstSexp

-- |Show an s-expression with an offset to the first line (not
-- printed) and an expected offset to the rest.
showSExpOff :: Bool -> Int -> Int -> SExp -> String
showSExpOff shouldShowParen initOff off e
  | off + length msg > 80 = msg
  | initOff + length raw <= 72 = raw
  | otherwise =
      case e of
        Rec name entries -> expShowRecord initOff off name entries
        Str xs -> "\"" ++ xs ++ "\""
        Case entries -> expShowCase initOff off entries
        Vec xs -> expShowCollection initOff off "[" "]" "," xs
        Tup xs -> expShowCollection initOff off "(" ")" "," xs
        Sub xs -> expShowList initOff off paren " " xs where
          paren = if shouldShowParen then ("(",")") else ("","")
        Sym x -> x
  where
    msg = "... omitting deep nest ..."
    raw = showSExpOneLine shouldShowParen e

expShowCollection :: Int -> Int -> String -> String -> String -> [SExp] -> String
expShowCollection _ _ b e _ [] = b ++ e
expShowCollection initOff off  b e sep (l:ls) = if alignedFits
  then if null alignedBody
       then veryConstervativeAShow
       else conservativeAShow
  else veryConstervativeAShow
  where
    conservativeAShow = b
                        ++ initialOneLine ++ sep ++ "\n"
                        ++ concat alignedBody
    veryConstervativeAShow =
      b
      ++ concat (allButLast (++ ",") id $ showSexpConservBody <$> (l:ls))
      ++ "\n" ++ replicate off ' ' ++ e
    alignedFits :: Bool
    alignedFits = all (\x -> length x <= 72) alignedBody
                  && initOff + length initialOneLine <= 72
    alignedBody :: [String]
    alignedBody = allButLast (wrapInSeparators $ sep ++ "\n") (wrapInSeparators e)
                  $ showSexpAlignedBody <$> ls
      where
        wrapInSeparators final x = replicate (initOff + 1) ' ' ++ x ++ final
    initialOneLine = showSExpOneLine False l
    showSexpConservBody recs = "\n"
                               ++ replicate (off + 1) ' '
                               ++ showSExpOff False (off + 1) (off + 2) recs
    showSexpAlignedBody s = showSExpOneLine False s

allButLast :: (a -> b) -> (a -> b) -> [a] -> [b]
allButLast f g = \case
  []   -> []
  [x]  -> [g x]
  x:xs -> f x:allButLast f g xs
allButFirst :: (a -> b) -> (a -> b) -> [a] -> [b]
allButFirst f g = \case
  []   -> []
  x:xs -> f x:(g <$> xs)


-- Args: beginning, end, separator, expressions
-- XXX: ashow [NodeReport{nrRef=N 170, nrMatState=Initial NoMat, nrSize=[TableSize{tableSizeRows=1024,tableSizeRowSize=108}], nrScore=1.0, nrIsInterm=False, nrQuery=[Q2 QProd (Q2 QProd (Q2 QProd (Q0 1) (Q0 2)) (Q0 11)) (Q0 4) :: Query (Int, Int) Int]}]
expShowList :: Int -> Int -> (String, String) -> String -> [SExp] -> String
expShowList _ _ (b,e) _ [] = b ++ e
expShowList initOff off (b, e) _ [l] =
  b ++ showSExpOff True (initOff+1) (off+1) l ++ e
expShowList initOff off (b, e) sep (l:ls) =
  if all (fits off) rawlines
  then case rawlines of
    []  -> veryConservative
    [_] -> veryConservative
    _   -> conservative
  else veryConservative
  where
    -- |Does x fit in the line given the offset?
    fits off' x = length (take 73 x) <= 72 - length rawline - off'
    rawline :: String
    rawline = showSExpOneLine True l
    rawlines :: [String]
    rawlines = showSExpOneLine True <$> ls
    conservative :: String
    conservative = b ++ rawline ++ sep
      ++ intercalate (sep ++ "\n" ++ pref') rawlines ++ e
      where
        off' = length rawline + length sep + off + length b
        pref' = replicate off' ' '
    veryConservative :: String
    veryConservative = b ++ intercalate (sep ++ "\n" ++ pref') subexp ++ e
      where
        subexp :: [String]
        subexp = carExpression:cdrExpressions
          where
            carExpression=showSExpOff True (initOff+1) (initOff+1) l
            cdrExpressions=showSExpOff True (off + 2) (off + 2) <$> ls
        pref' = replicate (off + 2) ' '

type Sep = String
type Correl = String

expShowRecord :: Int -> Int -> String -> [(String,SExp)] -> String
expShowRecord = expShowCurly "," "="
expShowCase :: Int -> Int -> [(String,SExp)] -> String
expShowCase initOff off = expShowCurly ";" "->" initOff off "\\case"

expShowCurly :: Sep -> Correl ->  Int -> Int -> String -> [(String, SExp)] -> String
expShowCurly _ _ _ _ name [] = name ++ "{}"
expShowCurly sep correl initOff off name alist =
  if all (\x -> alignedOffLen + 1 + length x <= 72) oneLineFields then concat
    $ allButFirst ((name ++ "{") ++) (replicate alignedOffLen ' ' ++)
    $ allButLast (++ (sep ++ "\n")) (++ "}") oneLineFields else name
    ++ "{\n"
    ++ concat conservativeRecordsWithFrame
    ++ replicate off ' '
    ++ "}"
  where
    alignedOffLen = initOff + length name + 1
    oneLineFields :: [String]
    oneLineFields = alist <&> \(l,v)
      -> printf "%s%s%s" l correl (showSExpOneLine False v)
    conservativeRecordsWithFrame
      :: [String]
    conservativeRecordsWithFrame =
      (replicate (2 + off) ' ' ++)
      <$> allButLast (++ (sep ++ "\n")) (++ "\n") conservativeRecords
    conservativeRecords = alist <&> \(l,v) -> printf
      "%s%s%s"
      l correl
      (showSExpOff False (off + 2 + length l + 1) (off + 2) v)

ashow :: AShow a => a -> String
ashow = showSExp . ashow'
-- :set -interactive-print=aprint
-- :set -interactive-print=print
aprint :: AShow x => x -> IO ()
aprint = putStrLn . ashow
gshow :: AShow a => a -> String
gshow = showSExpOneLine False . ashow'
