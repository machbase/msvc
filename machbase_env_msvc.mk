###############################################################################
# Copyright of this product 2013-2023,
# InfiniFlux Corporation(Incorporation) or its subsidiaries.
# All Rights reserved
###############################################################################
CC=cl /nologo /utf-8 /TC
LD_CC=link /nologo
CXX=cl /nologo /utf-8 /TP
LD_CXX=link /nologo
COMPILE_cxx=cl /nologo /utf-8 /TP /c  /W3 /EHsc /MT /Oi /Ot
COMPILE_cc=cl /nologo /utf-8 /TC /c  /W3 /EHsc /MT /Oi /Ot
DEF_OPT=/D
INC_OPT=/I
LIBDIR_OPT=/LIBPATH:
LIBDEF_OPT=/DEF:
LIB_AFT=.lib
LIB_OPT=
LD_LIBS=netapi32.lib advapi32.lib ws2_32.lib iphlpapi.lib dbghelp.lib shell32.lib user32.lib
AR_OUT_OPT=/OUT:
LD_OUT_OPT=/OUT:
CC_OUT_OPT=/Fo
AR_FLAGS=
CXX_DEFINES=/DALLREADY_HAVE_WINDOWS_TYPE
CC_DEFINES=/DALLREADY_HAVE_WINDOWS_TYPE
CC_FLAGS=/W3 /EHsc /MT /Oi /Ot
CXX_FLAGS=/W3 /EHsc /MT /Oi /Ot
SO_FLAGS=/DLL /INCREMENTAL:NO /OPT:REF /OPT:ICF /MANIFEST /MANIFESTFILE:$@.manifest /MAP:$@.map /MAPINFO:EXPORTS /MACHINE:X64
#LD_FLAGS=/INCREMENTAL:NO /STACK:2097152 /OPT:REF /OPT:ICF /MANIFEST /MANIFESTFILE:$@.manifest /MAP:$@.map /MAPINFO:EXPORTS /NODEFAULTLIB:LIBCMTD.lib /MACHINE:X64
LD_FLAGS=/INCREMENTAL:NO /STACK:2097152 /OPT:REF /OPT:ICF /NODEFAULTLIB:LIBCMTD.lib /MACHINE:X64
OBJ_SUF=.obj
EXEC_SUF=.exe
LIB_SUF=.lib
LIB_PRE=
INCLUDES = $(INC_OPT)$(MACHBASE_HOME)\include $(INC_OPT).
LIBDIRS = $(LIBDIR_OPT)$(MACHBASE_HOME)\lib
LD_FLAGS = $(LD_FLAGS) $(LIBDIRS)

.SUFFIXES : .c .obj

.c.obj:
	$(COMPILE_cc) $(INCLUDES) $(CC_OUT_OPT)$@ $<
	
#%.$(OBJ_SUF): %.c
#	$(COMPILE_cc) $(INCLUDES) $(CC_OUT_OPT)$@ $<

