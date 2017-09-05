# Copyright 2013-2023, InfiniFlux Corporation or its subsidiaries.
# All rights reserved.

# $Id:
#

MACHBASE_HOME=C:\machbase-3.5\machbase_home

include machbase_env_msvc.mk

INCLUDES = $(INC_OPT)$(MACHBASE_HOME)\include

all : make_data sample1_connect sample2_insert sample3_prepare sample4_append1 sample4_append2 make_data sample5_describe sample6_columns

make_data : make_data.obj
	$(LD_CC) $(LD_FLAGS) $(LD_OUT_OPT)$@$(EXEC_SUF) %s $(LIB_OPT)machbasecli_dll$(LIB_AFT)  $(LIBDIR_OPT)$(MACHBASE_HOME)/lib $(LD_LIBS)

make_data.obj : make_data.c
	$(COMPILE_cc) $(CC_FLAGS) $(INCLUDES) $(CC_OUT_OPT)$@ %s

sample1_connect : sample1_connect.obj
	$(LD_CC) $(LD_FLAGS) $(LD_OUT_OPT)$@$(EXEC_SUF) %s $(LIB_OPT)machbasecli_dll$(LIB_AFT)  $(LIBDIR_OPT)$(MACHBASE_HOME)/lib $(LD_LIBS)

sample1_connect.obj : sample1_connect.c
	$(COMPILE_cc) $(CC_FLAGS) $(INCLUDES) $(CC_OUT_OPT)$@ %s

sample2_insert : sample2_insert.obj
	$(LD_CC) $(LD_FLAGS) $(LD_OUT_OPT)$@$(EXEC_SUF) %s $(LIB_OPT)machbasecli_dll$(LIB_AFT)  $(LIBDIR_OPT)$(MACHBASE_HOME)/lib $(LD_LIBS)

sample2_insert.obj : sample2_insert.c
	$(COMPILE_cc) $(CC_FLAGS) $(INCLUDES) $(CC_OUT_OPT)$@ %s

sample3_prepare : sample3_prepare.obj
	$(LD_CC) $(LD_FLAGS) $(LD_OUT_OPT)$@$(EXEC_SUF) %s $(LIB_OPT)machbasecli_dll$(LIB_AFT)  $(LIBDIR_OPT)$(MACHBASE_HOME)/lib $(LD_LIBS)

sample3_prepare.obj : sample3_prepare.c
	$(COMPILE_cc) $(CC_FLAGS) $(INCLUDES) $(CC_OUT_OPT)$@ %s

sample4_append1 : sample4_append1.obj
	$(LD_CC) $(LD_FLAGS) $(LD_OUT_OPT)$@$(EXEC_SUF) %s $(LIB_OPT)machbasecli_dll$(LIB_AFT)  $(LIBDIR_OPT)$(MACHBASE_HOME)/lib $(LD_LIBS)

sample4_append1.obj : sample4_append1.c
	$(COMPILE_cc) $(CC_FLAGS) $(INCLUDES) $(CC_OUT_OPT)$@ %s

sample4_append2 : sample4_append2.obj
	$(LD_CC) $(LD_FLAGS) $(LD_OUT_OPT)$@$(EXEC_SUF) %s $(LIB_OPT)machbasecli_dll$(LIB_AFT)  $(LIBDIR_OPT)$(MACHBASE_HOME)/lib $(LD_LIBS)

sample4_append2.obj : sample4_append2.c
	$(COMPILE_cc) $(CC_FLAGS) $(INCLUDES) $(CC_OUT_OPT)$@ %s

sample5_describe : sample5_describe.obj
	$(LD_CC) $(LD_FLAGS) $(LD_OUT_OPT)$@$(EXEC_SUF) %s $(LIB_OPT)machbasecli_dll$(LIB_AFT)  $(LIBDIR_OPT)$(MACHBASE_HOME)/lib $(LD_LIBS)

sample5_describe.obj : sample5_describe.c
	$(COMPILE_cc) $(CC_FLAGS) $(INCLUDES) $(CC_OUT_OPT)$@ %s

sample6_columns : sample6_columns.obj
	$(LD_CC) $(LD_FLAGS) $(LD_OUT_OPT)$@$(EXEC_SUF) %s $(LIB_OPT)machbasecli_dll$(LIB_AFT)  $(LIBDIR_OPT)$(MACHBASE_HOME)/lib $(LD_LIBS)

sample6_columns.obj : sample6_columns.c
	$(COMPILE_cc) $(CC_FLAGS) $(INCLUDES) $(CC_OUT_OPT)$@ %s

clean :
	del *.obj *.txt *.exe
