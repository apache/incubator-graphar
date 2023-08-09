#!/bin/bash

workdir=/tmp/install-llvm
mkdir -p $workdir

mkdir -p $LLVM11_HOME/include/binutils
curl -L --output $LLVM11_HOME/include/binutils/plugin-api.h \
        https://raw.githubusercontent.com/bminor/binutils-gdb/binutils-2_37-branch/include/plugin-api.h

cd $workdir
curl -L -O https://github.com/llvm/llvm-project/releases/download/llvmorg-$LLVM_VER/llvm-project-$LLVM_VER.tar.xz
tar xf llvm-project-$LLVM_VER.tar.xz
cd llvm-project-$LLVM_VER
mkdir build
cd build
cmake ../llvm -DLLVM_ENABLE_PROJECTS="clang;llvm;lld" \
              -DLLVM_ENABLE_TERMINFO=OFF \
              -DLLVM_TARGETS_TO_BUILD=X86 \
              -DCMAKE_BUILD_TYPE=MinSizeRel \
              -DCMAKE_INSTALL_PREFIX=$LLVM11_HOME \
              -DLLVM_BINUTILS_INCDIR=$LLVM11_HOME/include/binutils \
              -DCMAKE_CXX_FLAGS_MINSIZEREL="-Os -DNDEBUG -static-libgcc -static-libstdc++ -s"
make install -j`nproc`

# cleanup
cd /
rm -rf $workdir
