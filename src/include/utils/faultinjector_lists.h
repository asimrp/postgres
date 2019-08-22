/*
 * faultinjector_lists.h
 *
 * List of fault injector types, states and some other things. These are
 * listed using C preprocessor macros. To use, you must define the appropriate
 * FI_* macros before #including this file.
 *
 * For example, to get an array of all the type strings, do:
 *
 * const char *FaultInjectorTypeStrings[] = {
 * #define FI_TYPE(id, str) str,
 * #include "utils/faultinjector_lists.h"
 * #undef FI_TYPE
 * };
 *
 *
 * To add a new entry, simple add a new FI_* line to the appropriate list
 * below.
 *
 *
 */

/* there is deliberately not an #ifndef FAULTINJECTOR_LISTS_H here */


/*
 * Fault types. These indicate the action to do when the fault injection
 * point is reached.
 */
#ifdef FI_TYPE
FI_TYPE(FaultInjectorTypeNotSpecified = 0, "")
FI_TYPE(FaultInjectorTypeSleep, "sleep")
FI_TYPE(FaultInjectorTypeFatal, "fatal")
FI_TYPE(FaultInjectorTypePanic, "panic")
FI_TYPE(FaultInjectorTypeError, "error")
FI_TYPE(FaultInjectorTypeInfiniteLoop, "infinite_loop")
FI_TYPE(FaultInjectorTypeSuspend, "suspend")
FI_TYPE(FaultInjectorTypeResume, "resume")
FI_TYPE(FaultInjectorTypeSkip, "skip")
FI_TYPE(FaultInjectorTypeReset, "reset")
FI_TYPE(FaultInjectorTypeStatus, "status")
FI_TYPE(FaultInjectorTypeSegv, "segv")
FI_TYPE(FaultInjectorTypeInterrupt, "interrupt")
FI_TYPE(FaultInjectorTypeWaitUntilTriggered, "wait_until_triggered")
#endif

/*
 *
 */
#ifdef FI_DDL_STATEMENT
FI_DDL_STATEMENT(DDLNotSpecified = 0, "")
FI_DDL_STATEMENT(CreateDatabase, "create_database")
FI_DDL_STATEMENT(DropDatabase, "drop_database")
FI_DDL_STATEMENT(CreateTable, "create_table")
FI_DDL_STATEMENT(DropTable, "drop_table")
FI_DDL_STATEMENT(CreateIndex, "create_index")
FI_DDL_STATEMENT(AlterIndex, "alter_index")
FI_DDL_STATEMENT(ReIndex, "reindex")
FI_DDL_STATEMENT(DropIndex, "drop_index")
FI_DDL_STATEMENT(CreateTablespaces, "create_tablespaces")
FI_DDL_STATEMENT(DropTablespaces, "drop_tablespaces")
FI_DDL_STATEMENT(Truncate, "truncate")
FI_DDL_STATEMENT(Vacuum, "vacuum")
#endif

/*
 * States of a fault.
 */
#ifdef FI_STATE
FI_STATE(FaultInjectorStateNotInitialized = 0, "not initialized")

/* The fault has been injected (enabled using the SQL interface). */
FI_STATE(FaultInjectorStateWaiting, "set")

/*
 * A backend process reached the fault point that was set and the
 * corresponding action has been taken.
 */
FI_STATE(FaultInjectorStateTriggered, "triggered")

/*
 * The fault has been triggered as many times as was configured by the
 * SQL interface.  The action associated with it will no longer be taken
 * if it is reached during execution.
 */
FI_STATE(FaultInjectorStateCompleted, "completed")

/* Fault was NOT injected */
FI_STATE(FaultInjectorStateFailed, "failed")
#endif
