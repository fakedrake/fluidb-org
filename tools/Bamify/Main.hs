{-# LANGUAGE CPP                 #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeFamilies        #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}

module Main (main) where

import           FluiDB.Bamify.Main

deathByWrongArgs :: [String] -> IO ()
deathByWrongArgs args =
  die $
  printf ("Expected: <infile> {--tobintable,--bamifycode} <outfile>\n" ++
          "found: %s") $ unwords args


bamifyMain :: IO ()
bamifyMain = do
  args <- getArgs
  if length args < 3
    then deathByWrongArgs args
    else do
    let tableFile:cmd:outFile:_ = args
    case cmd of
      "--tobintable" ->
        mainBinTable tableFile outFile
      "--bamifycode" ->
        mainBamifyCode tableFile outFile
      _ -> deathByWrongArgs args
