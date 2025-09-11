---
id: java
title: Java Library
sidebar_position: 1
---

# Java Library

The GraphAr Java library is currently available in two versions:

## 1. Java-FFI Version (Deprecated)

This version is implemented using FastFFI to bridge Java and C++ code. While functional, this version is no longer being actively updated or maintained.

## 2. Pure Java Version (Under Development)

This is the next generation of the GraphAr Java library, being actively developed to provide a pure Java implementation without native dependencies.

Currently, only the `graphar-info` module has been implemented in pure Java, which provides the ability to parse graph info (schema). Additional modules such as IO and high level API will be provided progressively.
