package org.dbiir.txnsails.parse;// Generated from PostgreSQLParser.g4 by ANTLR 4.13.2
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link PostgreSQLParser}.
 */
public interface PostgreSQLParserListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#root}.
	 * @param ctx the parse tree
	 */
	void enterRoot(PostgreSQLParser.RootContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#root}.
	 * @param ctx the parse tree
	 */
	void exitRoot(PostgreSQLParser.RootContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#stmtblock}.
	 * @param ctx the parse tree
	 */
	void enterStmtblock(PostgreSQLParser.StmtblockContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#stmtblock}.
	 * @param ctx the parse tree
	 */
	void exitStmtblock(PostgreSQLParser.StmtblockContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#stmtmulti}.
	 * @param ctx the parse tree
	 */
	void enterStmtmulti(PostgreSQLParser.StmtmultiContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#stmtmulti}.
	 * @param ctx the parse tree
	 */
	void exitStmtmulti(PostgreSQLParser.StmtmultiContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#stmt}.
	 * @param ctx the parse tree
	 */
	void enterStmt(PostgreSQLParser.StmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#stmt}.
	 * @param ctx the parse tree
	 */
	void exitStmt(PostgreSQLParser.StmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#callstmt}.
	 * @param ctx the parse tree
	 */
	void enterCallstmt(PostgreSQLParser.CallstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#callstmt}.
	 * @param ctx the parse tree
	 */
	void exitCallstmt(PostgreSQLParser.CallstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createrolestmt}.
	 * @param ctx the parse tree
	 */
	void enterCreaterolestmt(PostgreSQLParser.CreaterolestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createrolestmt}.
	 * @param ctx the parse tree
	 */
	void exitCreaterolestmt(PostgreSQLParser.CreaterolestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#with_}.
	 * @param ctx the parse tree
	 */
	void enterWith_(PostgreSQLParser.With_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#with_}.
	 * @param ctx the parse tree
	 */
	void exitWith_(PostgreSQLParser.With_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#optrolelist}.
	 * @param ctx the parse tree
	 */
	void enterOptrolelist(PostgreSQLParser.OptrolelistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#optrolelist}.
	 * @param ctx the parse tree
	 */
	void exitOptrolelist(PostgreSQLParser.OptrolelistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alteroptrolelist}.
	 * @param ctx the parse tree
	 */
	void enterAlteroptrolelist(PostgreSQLParser.AlteroptrolelistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alteroptrolelist}.
	 * @param ctx the parse tree
	 */
	void exitAlteroptrolelist(PostgreSQLParser.AlteroptrolelistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alteroptroleelem}.
	 * @param ctx the parse tree
	 */
	void enterAlteroptroleelem(PostgreSQLParser.AlteroptroleelemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alteroptroleelem}.
	 * @param ctx the parse tree
	 */
	void exitAlteroptroleelem(PostgreSQLParser.AlteroptroleelemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createoptroleelem}.
	 * @param ctx the parse tree
	 */
	void enterCreateoptroleelem(PostgreSQLParser.CreateoptroleelemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createoptroleelem}.
	 * @param ctx the parse tree
	 */
	void exitCreateoptroleelem(PostgreSQLParser.CreateoptroleelemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createuserstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreateuserstmt(PostgreSQLParser.CreateuserstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createuserstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreateuserstmt(PostgreSQLParser.CreateuserstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterrolestmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterrolestmt(PostgreSQLParser.AlterrolestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterrolestmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterrolestmt(PostgreSQLParser.AlterrolestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#in_database_}.
	 * @param ctx the parse tree
	 */
	void enterIn_database_(PostgreSQLParser.In_database_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#in_database_}.
	 * @param ctx the parse tree
	 */
	void exitIn_database_(PostgreSQLParser.In_database_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterrolesetstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterrolesetstmt(PostgreSQLParser.AlterrolesetstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterrolesetstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterrolesetstmt(PostgreSQLParser.AlterrolesetstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#droprolestmt}.
	 * @param ctx the parse tree
	 */
	void enterDroprolestmt(PostgreSQLParser.DroprolestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#droprolestmt}.
	 * @param ctx the parse tree
	 */
	void exitDroprolestmt(PostgreSQLParser.DroprolestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#creategroupstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreategroupstmt(PostgreSQLParser.CreategroupstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#creategroupstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreategroupstmt(PostgreSQLParser.CreategroupstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#altergroupstmt}.
	 * @param ctx the parse tree
	 */
	void enterAltergroupstmt(PostgreSQLParser.AltergroupstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#altergroupstmt}.
	 * @param ctx the parse tree
	 */
	void exitAltergroupstmt(PostgreSQLParser.AltergroupstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#add_drop}.
	 * @param ctx the parse tree
	 */
	void enterAdd_drop(PostgreSQLParser.Add_dropContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#add_drop}.
	 * @param ctx the parse tree
	 */
	void exitAdd_drop(PostgreSQLParser.Add_dropContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createschemastmt}.
	 * @param ctx the parse tree
	 */
	void enterCreateschemastmt(PostgreSQLParser.CreateschemastmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createschemastmt}.
	 * @param ctx the parse tree
	 */
	void exitCreateschemastmt(PostgreSQLParser.CreateschemastmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#optschemaname}.
	 * @param ctx the parse tree
	 */
	void enterOptschemaname(PostgreSQLParser.OptschemanameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#optschemaname}.
	 * @param ctx the parse tree
	 */
	void exitOptschemaname(PostgreSQLParser.OptschemanameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#optschemaeltlist}.
	 * @param ctx the parse tree
	 */
	void enterOptschemaeltlist(PostgreSQLParser.OptschemaeltlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#optschemaeltlist}.
	 * @param ctx the parse tree
	 */
	void exitOptschemaeltlist(PostgreSQLParser.OptschemaeltlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#schema_stmt}.
	 * @param ctx the parse tree
	 */
	void enterSchema_stmt(PostgreSQLParser.Schema_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#schema_stmt}.
	 * @param ctx the parse tree
	 */
	void exitSchema_stmt(PostgreSQLParser.Schema_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#variablesetstmt}.
	 * @param ctx the parse tree
	 */
	void enterVariablesetstmt(PostgreSQLParser.VariablesetstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#variablesetstmt}.
	 * @param ctx the parse tree
	 */
	void exitVariablesetstmt(PostgreSQLParser.VariablesetstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#set_rest}.
	 * @param ctx the parse tree
	 */
	void enterSet_rest(PostgreSQLParser.Set_restContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#set_rest}.
	 * @param ctx the parse tree
	 */
	void exitSet_rest(PostgreSQLParser.Set_restContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#generic_set}.
	 * @param ctx the parse tree
	 */
	void enterGeneric_set(PostgreSQLParser.Generic_setContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#generic_set}.
	 * @param ctx the parse tree
	 */
	void exitGeneric_set(PostgreSQLParser.Generic_setContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#set_rest_more}.
	 * @param ctx the parse tree
	 */
	void enterSet_rest_more(PostgreSQLParser.Set_rest_moreContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#set_rest_more}.
	 * @param ctx the parse tree
	 */
	void exitSet_rest_more(PostgreSQLParser.Set_rest_moreContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#var_name}.
	 * @param ctx the parse tree
	 */
	void enterVar_name(PostgreSQLParser.Var_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#var_name}.
	 * @param ctx the parse tree
	 */
	void exitVar_name(PostgreSQLParser.Var_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#var_list}.
	 * @param ctx the parse tree
	 */
	void enterVar_list(PostgreSQLParser.Var_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#var_list}.
	 * @param ctx the parse tree
	 */
	void exitVar_list(PostgreSQLParser.Var_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#var_value}.
	 * @param ctx the parse tree
	 */
	void enterVar_value(PostgreSQLParser.Var_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#var_value}.
	 * @param ctx the parse tree
	 */
	void exitVar_value(PostgreSQLParser.Var_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#iso_level}.
	 * @param ctx the parse tree
	 */
	void enterIso_level(PostgreSQLParser.Iso_levelContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#iso_level}.
	 * @param ctx the parse tree
	 */
	void exitIso_level(PostgreSQLParser.Iso_levelContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#boolean_or_string_}.
	 * @param ctx the parse tree
	 */
	void enterBoolean_or_string_(PostgreSQLParser.Boolean_or_string_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#boolean_or_string_}.
	 * @param ctx the parse tree
	 */
	void exitBoolean_or_string_(PostgreSQLParser.Boolean_or_string_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#zone_value}.
	 * @param ctx the parse tree
	 */
	void enterZone_value(PostgreSQLParser.Zone_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#zone_value}.
	 * @param ctx the parse tree
	 */
	void exitZone_value(PostgreSQLParser.Zone_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#encoding_}.
	 * @param ctx the parse tree
	 */
	void enterEncoding_(PostgreSQLParser.Encoding_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#encoding_}.
	 * @param ctx the parse tree
	 */
	void exitEncoding_(PostgreSQLParser.Encoding_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#nonreservedword_or_sconst}.
	 * @param ctx the parse tree
	 */
	void enterNonreservedword_or_sconst(PostgreSQLParser.Nonreservedword_or_sconstContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#nonreservedword_or_sconst}.
	 * @param ctx the parse tree
	 */
	void exitNonreservedword_or_sconst(PostgreSQLParser.Nonreservedword_or_sconstContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#variableresetstmt}.
	 * @param ctx the parse tree
	 */
	void enterVariableresetstmt(PostgreSQLParser.VariableresetstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#variableresetstmt}.
	 * @param ctx the parse tree
	 */
	void exitVariableresetstmt(PostgreSQLParser.VariableresetstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#reset_rest}.
	 * @param ctx the parse tree
	 */
	void enterReset_rest(PostgreSQLParser.Reset_restContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#reset_rest}.
	 * @param ctx the parse tree
	 */
	void exitReset_rest(PostgreSQLParser.Reset_restContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#generic_reset}.
	 * @param ctx the parse tree
	 */
	void enterGeneric_reset(PostgreSQLParser.Generic_resetContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#generic_reset}.
	 * @param ctx the parse tree
	 */
	void exitGeneric_reset(PostgreSQLParser.Generic_resetContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#setresetclause}.
	 * @param ctx the parse tree
	 */
	void enterSetresetclause(PostgreSQLParser.SetresetclauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#setresetclause}.
	 * @param ctx the parse tree
	 */
	void exitSetresetclause(PostgreSQLParser.SetresetclauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#functionsetresetclause}.
	 * @param ctx the parse tree
	 */
	void enterFunctionsetresetclause(PostgreSQLParser.FunctionsetresetclauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#functionsetresetclause}.
	 * @param ctx the parse tree
	 */
	void exitFunctionsetresetclause(PostgreSQLParser.FunctionsetresetclauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#variableshowstmt}.
	 * @param ctx the parse tree
	 */
	void enterVariableshowstmt(PostgreSQLParser.VariableshowstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#variableshowstmt}.
	 * @param ctx the parse tree
	 */
	void exitVariableshowstmt(PostgreSQLParser.VariableshowstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#constraintssetstmt}.
	 * @param ctx the parse tree
	 */
	void enterConstraintssetstmt(PostgreSQLParser.ConstraintssetstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#constraintssetstmt}.
	 * @param ctx the parse tree
	 */
	void exitConstraintssetstmt(PostgreSQLParser.ConstraintssetstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#constraints_set_list}.
	 * @param ctx the parse tree
	 */
	void enterConstraints_set_list(PostgreSQLParser.Constraints_set_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#constraints_set_list}.
	 * @param ctx the parse tree
	 */
	void exitConstraints_set_list(PostgreSQLParser.Constraints_set_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#constraints_set_mode}.
	 * @param ctx the parse tree
	 */
	void enterConstraints_set_mode(PostgreSQLParser.Constraints_set_modeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#constraints_set_mode}.
	 * @param ctx the parse tree
	 */
	void exitConstraints_set_mode(PostgreSQLParser.Constraints_set_modeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#checkpointstmt}.
	 * @param ctx the parse tree
	 */
	void enterCheckpointstmt(PostgreSQLParser.CheckpointstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#checkpointstmt}.
	 * @param ctx the parse tree
	 */
	void exitCheckpointstmt(PostgreSQLParser.CheckpointstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#discardstmt}.
	 * @param ctx the parse tree
	 */
	void enterDiscardstmt(PostgreSQLParser.DiscardstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#discardstmt}.
	 * @param ctx the parse tree
	 */
	void exitDiscardstmt(PostgreSQLParser.DiscardstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#altertablestmt}.
	 * @param ctx the parse tree
	 */
	void enterAltertablestmt(PostgreSQLParser.AltertablestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#altertablestmt}.
	 * @param ctx the parse tree
	 */
	void exitAltertablestmt(PostgreSQLParser.AltertablestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alter_table_cmds}.
	 * @param ctx the parse tree
	 */
	void enterAlter_table_cmds(PostgreSQLParser.Alter_table_cmdsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alter_table_cmds}.
	 * @param ctx the parse tree
	 */
	void exitAlter_table_cmds(PostgreSQLParser.Alter_table_cmdsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#partition_cmd}.
	 * @param ctx the parse tree
	 */
	void enterPartition_cmd(PostgreSQLParser.Partition_cmdContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#partition_cmd}.
	 * @param ctx the parse tree
	 */
	void exitPartition_cmd(PostgreSQLParser.Partition_cmdContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#index_partition_cmd}.
	 * @param ctx the parse tree
	 */
	void enterIndex_partition_cmd(PostgreSQLParser.Index_partition_cmdContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#index_partition_cmd}.
	 * @param ctx the parse tree
	 */
	void exitIndex_partition_cmd(PostgreSQLParser.Index_partition_cmdContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alter_table_cmd}.
	 * @param ctx the parse tree
	 */
	void enterAlter_table_cmd(PostgreSQLParser.Alter_table_cmdContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alter_table_cmd}.
	 * @param ctx the parse tree
	 */
	void exitAlter_table_cmd(PostgreSQLParser.Alter_table_cmdContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alter_column_default}.
	 * @param ctx the parse tree
	 */
	void enterAlter_column_default(PostgreSQLParser.Alter_column_defaultContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alter_column_default}.
	 * @param ctx the parse tree
	 */
	void exitAlter_column_default(PostgreSQLParser.Alter_column_defaultContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#drop_behavior_}.
	 * @param ctx the parse tree
	 */
	void enterDrop_behavior_(PostgreSQLParser.Drop_behavior_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#drop_behavior_}.
	 * @param ctx the parse tree
	 */
	void exitDrop_behavior_(PostgreSQLParser.Drop_behavior_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#collate_clause_}.
	 * @param ctx the parse tree
	 */
	void enterCollate_clause_(PostgreSQLParser.Collate_clause_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#collate_clause_}.
	 * @param ctx the parse tree
	 */
	void exitCollate_clause_(PostgreSQLParser.Collate_clause_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alter_using}.
	 * @param ctx the parse tree
	 */
	void enterAlter_using(PostgreSQLParser.Alter_usingContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alter_using}.
	 * @param ctx the parse tree
	 */
	void exitAlter_using(PostgreSQLParser.Alter_usingContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#replica_identity}.
	 * @param ctx the parse tree
	 */
	void enterReplica_identity(PostgreSQLParser.Replica_identityContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#replica_identity}.
	 * @param ctx the parse tree
	 */
	void exitReplica_identity(PostgreSQLParser.Replica_identityContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#reloptions}.
	 * @param ctx the parse tree
	 */
	void enterReloptions(PostgreSQLParser.ReloptionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#reloptions}.
	 * @param ctx the parse tree
	 */
	void exitReloptions(PostgreSQLParser.ReloptionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#reloptions_}.
	 * @param ctx the parse tree
	 */
	void enterReloptions_(PostgreSQLParser.Reloptions_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#reloptions_}.
	 * @param ctx the parse tree
	 */
	void exitReloptions_(PostgreSQLParser.Reloptions_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#reloption_list}.
	 * @param ctx the parse tree
	 */
	void enterReloption_list(PostgreSQLParser.Reloption_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#reloption_list}.
	 * @param ctx the parse tree
	 */
	void exitReloption_list(PostgreSQLParser.Reloption_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#reloption_elem}.
	 * @param ctx the parse tree
	 */
	void enterReloption_elem(PostgreSQLParser.Reloption_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#reloption_elem}.
	 * @param ctx the parse tree
	 */
	void exitReloption_elem(PostgreSQLParser.Reloption_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alter_identity_column_option_list}.
	 * @param ctx the parse tree
	 */
	void enterAlter_identity_column_option_list(PostgreSQLParser.Alter_identity_column_option_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alter_identity_column_option_list}.
	 * @param ctx the parse tree
	 */
	void exitAlter_identity_column_option_list(PostgreSQLParser.Alter_identity_column_option_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alter_identity_column_option}.
	 * @param ctx the parse tree
	 */
	void enterAlter_identity_column_option(PostgreSQLParser.Alter_identity_column_optionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alter_identity_column_option}.
	 * @param ctx the parse tree
	 */
	void exitAlter_identity_column_option(PostgreSQLParser.Alter_identity_column_optionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#partitionboundspec}.
	 * @param ctx the parse tree
	 */
	void enterPartitionboundspec(PostgreSQLParser.PartitionboundspecContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#partitionboundspec}.
	 * @param ctx the parse tree
	 */
	void exitPartitionboundspec(PostgreSQLParser.PartitionboundspecContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#hash_partbound_elem}.
	 * @param ctx the parse tree
	 */
	void enterHash_partbound_elem(PostgreSQLParser.Hash_partbound_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#hash_partbound_elem}.
	 * @param ctx the parse tree
	 */
	void exitHash_partbound_elem(PostgreSQLParser.Hash_partbound_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#hash_partbound}.
	 * @param ctx the parse tree
	 */
	void enterHash_partbound(PostgreSQLParser.Hash_partboundContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#hash_partbound}.
	 * @param ctx the parse tree
	 */
	void exitHash_partbound(PostgreSQLParser.Hash_partboundContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#altercompositetypestmt}.
	 * @param ctx the parse tree
	 */
	void enterAltercompositetypestmt(PostgreSQLParser.AltercompositetypestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#altercompositetypestmt}.
	 * @param ctx the parse tree
	 */
	void exitAltercompositetypestmt(PostgreSQLParser.AltercompositetypestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alter_type_cmds}.
	 * @param ctx the parse tree
	 */
	void enterAlter_type_cmds(PostgreSQLParser.Alter_type_cmdsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alter_type_cmds}.
	 * @param ctx the parse tree
	 */
	void exitAlter_type_cmds(PostgreSQLParser.Alter_type_cmdsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alter_type_cmd}.
	 * @param ctx the parse tree
	 */
	void enterAlter_type_cmd(PostgreSQLParser.Alter_type_cmdContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alter_type_cmd}.
	 * @param ctx the parse tree
	 */
	void exitAlter_type_cmd(PostgreSQLParser.Alter_type_cmdContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#closeportalstmt}.
	 * @param ctx the parse tree
	 */
	void enterCloseportalstmt(PostgreSQLParser.CloseportalstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#closeportalstmt}.
	 * @param ctx the parse tree
	 */
	void exitCloseportalstmt(PostgreSQLParser.CloseportalstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#copystmt}.
	 * @param ctx the parse tree
	 */
	void enterCopystmt(PostgreSQLParser.CopystmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#copystmt}.
	 * @param ctx the parse tree
	 */
	void exitCopystmt(PostgreSQLParser.CopystmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#copy_from}.
	 * @param ctx the parse tree
	 */
	void enterCopy_from(PostgreSQLParser.Copy_fromContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#copy_from}.
	 * @param ctx the parse tree
	 */
	void exitCopy_from(PostgreSQLParser.Copy_fromContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#program_}.
	 * @param ctx the parse tree
	 */
	void enterProgram_(PostgreSQLParser.Program_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#program_}.
	 * @param ctx the parse tree
	 */
	void exitProgram_(PostgreSQLParser.Program_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#copy_file_name}.
	 * @param ctx the parse tree
	 */
	void enterCopy_file_name(PostgreSQLParser.Copy_file_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#copy_file_name}.
	 * @param ctx the parse tree
	 */
	void exitCopy_file_name(PostgreSQLParser.Copy_file_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#copy_options}.
	 * @param ctx the parse tree
	 */
	void enterCopy_options(PostgreSQLParser.Copy_optionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#copy_options}.
	 * @param ctx the parse tree
	 */
	void exitCopy_options(PostgreSQLParser.Copy_optionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#copy_opt_list}.
	 * @param ctx the parse tree
	 */
	void enterCopy_opt_list(PostgreSQLParser.Copy_opt_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#copy_opt_list}.
	 * @param ctx the parse tree
	 */
	void exitCopy_opt_list(PostgreSQLParser.Copy_opt_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#copy_opt_item}.
	 * @param ctx the parse tree
	 */
	void enterCopy_opt_item(PostgreSQLParser.Copy_opt_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#copy_opt_item}.
	 * @param ctx the parse tree
	 */
	void exitCopy_opt_item(PostgreSQLParser.Copy_opt_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#binary_}.
	 * @param ctx the parse tree
	 */
	void enterBinary_(PostgreSQLParser.Binary_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#binary_}.
	 * @param ctx the parse tree
	 */
	void exitBinary_(PostgreSQLParser.Binary_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#copy_delimiter}.
	 * @param ctx the parse tree
	 */
	void enterCopy_delimiter(PostgreSQLParser.Copy_delimiterContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#copy_delimiter}.
	 * @param ctx the parse tree
	 */
	void exitCopy_delimiter(PostgreSQLParser.Copy_delimiterContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#using_}.
	 * @param ctx the parse tree
	 */
	void enterUsing_(PostgreSQLParser.Using_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#using_}.
	 * @param ctx the parse tree
	 */
	void exitUsing_(PostgreSQLParser.Using_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#copy_generic_opt_list}.
	 * @param ctx the parse tree
	 */
	void enterCopy_generic_opt_list(PostgreSQLParser.Copy_generic_opt_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#copy_generic_opt_list}.
	 * @param ctx the parse tree
	 */
	void exitCopy_generic_opt_list(PostgreSQLParser.Copy_generic_opt_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#copy_generic_opt_elem}.
	 * @param ctx the parse tree
	 */
	void enterCopy_generic_opt_elem(PostgreSQLParser.Copy_generic_opt_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#copy_generic_opt_elem}.
	 * @param ctx the parse tree
	 */
	void exitCopy_generic_opt_elem(PostgreSQLParser.Copy_generic_opt_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#copy_generic_opt_arg}.
	 * @param ctx the parse tree
	 */
	void enterCopy_generic_opt_arg(PostgreSQLParser.Copy_generic_opt_argContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#copy_generic_opt_arg}.
	 * @param ctx the parse tree
	 */
	void exitCopy_generic_opt_arg(PostgreSQLParser.Copy_generic_opt_argContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#copy_generic_opt_arg_list}.
	 * @param ctx the parse tree
	 */
	void enterCopy_generic_opt_arg_list(PostgreSQLParser.Copy_generic_opt_arg_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#copy_generic_opt_arg_list}.
	 * @param ctx the parse tree
	 */
	void exitCopy_generic_opt_arg_list(PostgreSQLParser.Copy_generic_opt_arg_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#copy_generic_opt_arg_list_item}.
	 * @param ctx the parse tree
	 */
	void enterCopy_generic_opt_arg_list_item(PostgreSQLParser.Copy_generic_opt_arg_list_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#copy_generic_opt_arg_list_item}.
	 * @param ctx the parse tree
	 */
	void exitCopy_generic_opt_arg_list_item(PostgreSQLParser.Copy_generic_opt_arg_list_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreatestmt(PostgreSQLParser.CreatestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreatestmt(PostgreSQLParser.CreatestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#opttemp}.
	 * @param ctx the parse tree
	 */
	void enterOpttemp(PostgreSQLParser.OpttempContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#opttemp}.
	 * @param ctx the parse tree
	 */
	void exitOpttemp(PostgreSQLParser.OpttempContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#opttableelementlist}.
	 * @param ctx the parse tree
	 */
	void enterOpttableelementlist(PostgreSQLParser.OpttableelementlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#opttableelementlist}.
	 * @param ctx the parse tree
	 */
	void exitOpttableelementlist(PostgreSQLParser.OpttableelementlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#opttypedtableelementlist}.
	 * @param ctx the parse tree
	 */
	void enterOpttypedtableelementlist(PostgreSQLParser.OpttypedtableelementlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#opttypedtableelementlist}.
	 * @param ctx the parse tree
	 */
	void exitOpttypedtableelementlist(PostgreSQLParser.OpttypedtableelementlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#tableelementlist}.
	 * @param ctx the parse tree
	 */
	void enterTableelementlist(PostgreSQLParser.TableelementlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#tableelementlist}.
	 * @param ctx the parse tree
	 */
	void exitTableelementlist(PostgreSQLParser.TableelementlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#typedtableelementlist}.
	 * @param ctx the parse tree
	 */
	void enterTypedtableelementlist(PostgreSQLParser.TypedtableelementlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#typedtableelementlist}.
	 * @param ctx the parse tree
	 */
	void exitTypedtableelementlist(PostgreSQLParser.TypedtableelementlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#tableelement}.
	 * @param ctx the parse tree
	 */
	void enterTableelement(PostgreSQLParser.TableelementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#tableelement}.
	 * @param ctx the parse tree
	 */
	void exitTableelement(PostgreSQLParser.TableelementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#typedtableelement}.
	 * @param ctx the parse tree
	 */
	void enterTypedtableelement(PostgreSQLParser.TypedtableelementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#typedtableelement}.
	 * @param ctx the parse tree
	 */
	void exitTypedtableelement(PostgreSQLParser.TypedtableelementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#columnDef}.
	 * @param ctx the parse tree
	 */
	void enterColumnDef(PostgreSQLParser.ColumnDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#columnDef}.
	 * @param ctx the parse tree
	 */
	void exitColumnDef(PostgreSQLParser.ColumnDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#columnOptions}.
	 * @param ctx the parse tree
	 */
	void enterColumnOptions(PostgreSQLParser.ColumnOptionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#columnOptions}.
	 * @param ctx the parse tree
	 */
	void exitColumnOptions(PostgreSQLParser.ColumnOptionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#colquallist}.
	 * @param ctx the parse tree
	 */
	void enterColquallist(PostgreSQLParser.ColquallistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#colquallist}.
	 * @param ctx the parse tree
	 */
	void exitColquallist(PostgreSQLParser.ColquallistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#colconstraint}.
	 * @param ctx the parse tree
	 */
	void enterColconstraint(PostgreSQLParser.ColconstraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#colconstraint}.
	 * @param ctx the parse tree
	 */
	void exitColconstraint(PostgreSQLParser.ColconstraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#colconstraintelem}.
	 * @param ctx the parse tree
	 */
	void enterColconstraintelem(PostgreSQLParser.ColconstraintelemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#colconstraintelem}.
	 * @param ctx the parse tree
	 */
	void exitColconstraintelem(PostgreSQLParser.ColconstraintelemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#generated_when}.
	 * @param ctx the parse tree
	 */
	void enterGenerated_when(PostgreSQLParser.Generated_whenContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#generated_when}.
	 * @param ctx the parse tree
	 */
	void exitGenerated_when(PostgreSQLParser.Generated_whenContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#constraintattr}.
	 * @param ctx the parse tree
	 */
	void enterConstraintattr(PostgreSQLParser.ConstraintattrContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#constraintattr}.
	 * @param ctx the parse tree
	 */
	void exitConstraintattr(PostgreSQLParser.ConstraintattrContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#tablelikeclause}.
	 * @param ctx the parse tree
	 */
	void enterTablelikeclause(PostgreSQLParser.TablelikeclauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#tablelikeclause}.
	 * @param ctx the parse tree
	 */
	void exitTablelikeclause(PostgreSQLParser.TablelikeclauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#tablelikeoptionlist}.
	 * @param ctx the parse tree
	 */
	void enterTablelikeoptionlist(PostgreSQLParser.TablelikeoptionlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#tablelikeoptionlist}.
	 * @param ctx the parse tree
	 */
	void exitTablelikeoptionlist(PostgreSQLParser.TablelikeoptionlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#tablelikeoption}.
	 * @param ctx the parse tree
	 */
	void enterTablelikeoption(PostgreSQLParser.TablelikeoptionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#tablelikeoption}.
	 * @param ctx the parse tree
	 */
	void exitTablelikeoption(PostgreSQLParser.TablelikeoptionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#tableconstraint}.
	 * @param ctx the parse tree
	 */
	void enterTableconstraint(PostgreSQLParser.TableconstraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#tableconstraint}.
	 * @param ctx the parse tree
	 */
	void exitTableconstraint(PostgreSQLParser.TableconstraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#constraintelem}.
	 * @param ctx the parse tree
	 */
	void enterConstraintelem(PostgreSQLParser.ConstraintelemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#constraintelem}.
	 * @param ctx the parse tree
	 */
	void exitConstraintelem(PostgreSQLParser.ConstraintelemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#no_inherit_}.
	 * @param ctx the parse tree
	 */
	void enterNo_inherit_(PostgreSQLParser.No_inherit_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#no_inherit_}.
	 * @param ctx the parse tree
	 */
	void exitNo_inherit_(PostgreSQLParser.No_inherit_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#column_list_}.
	 * @param ctx the parse tree
	 */
	void enterColumn_list_(PostgreSQLParser.Column_list_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#column_list_}.
	 * @param ctx the parse tree
	 */
	void exitColumn_list_(PostgreSQLParser.Column_list_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#columnlist}.
	 * @param ctx the parse tree
	 */
	void enterColumnlist(PostgreSQLParser.ColumnlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#columnlist}.
	 * @param ctx the parse tree
	 */
	void exitColumnlist(PostgreSQLParser.ColumnlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#columnElem}.
	 * @param ctx the parse tree
	 */
	void enterColumnElem(PostgreSQLParser.ColumnElemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#columnElem}.
	 * @param ctx the parse tree
	 */
	void exitColumnElem(PostgreSQLParser.ColumnElemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#c_include_}.
	 * @param ctx the parse tree
	 */
	void enterC_include_(PostgreSQLParser.C_include_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#c_include_}.
	 * @param ctx the parse tree
	 */
	void exitC_include_(PostgreSQLParser.C_include_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#key_match}.
	 * @param ctx the parse tree
	 */
	void enterKey_match(PostgreSQLParser.Key_matchContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#key_match}.
	 * @param ctx the parse tree
	 */
	void exitKey_match(PostgreSQLParser.Key_matchContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#exclusionconstraintlist}.
	 * @param ctx the parse tree
	 */
	void enterExclusionconstraintlist(PostgreSQLParser.ExclusionconstraintlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#exclusionconstraintlist}.
	 * @param ctx the parse tree
	 */
	void exitExclusionconstraintlist(PostgreSQLParser.ExclusionconstraintlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#exclusionconstraintelem}.
	 * @param ctx the parse tree
	 */
	void enterExclusionconstraintelem(PostgreSQLParser.ExclusionconstraintelemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#exclusionconstraintelem}.
	 * @param ctx the parse tree
	 */
	void exitExclusionconstraintelem(PostgreSQLParser.ExclusionconstraintelemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#exclusionwhereclause}.
	 * @param ctx the parse tree
	 */
	void enterExclusionwhereclause(PostgreSQLParser.ExclusionwhereclauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#exclusionwhereclause}.
	 * @param ctx the parse tree
	 */
	void exitExclusionwhereclause(PostgreSQLParser.ExclusionwhereclauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#key_actions}.
	 * @param ctx the parse tree
	 */
	void enterKey_actions(PostgreSQLParser.Key_actionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#key_actions}.
	 * @param ctx the parse tree
	 */
	void exitKey_actions(PostgreSQLParser.Key_actionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#key_update}.
	 * @param ctx the parse tree
	 */
	void enterKey_update(PostgreSQLParser.Key_updateContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#key_update}.
	 * @param ctx the parse tree
	 */
	void exitKey_update(PostgreSQLParser.Key_updateContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#key_delete}.
	 * @param ctx the parse tree
	 */
	void enterKey_delete(PostgreSQLParser.Key_deleteContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#key_delete}.
	 * @param ctx the parse tree
	 */
	void exitKey_delete(PostgreSQLParser.Key_deleteContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#key_action}.
	 * @param ctx the parse tree
	 */
	void enterKey_action(PostgreSQLParser.Key_actionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#key_action}.
	 * @param ctx the parse tree
	 */
	void exitKey_action(PostgreSQLParser.Key_actionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#optinherit}.
	 * @param ctx the parse tree
	 */
	void enterOptinherit(PostgreSQLParser.OptinheritContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#optinherit}.
	 * @param ctx the parse tree
	 */
	void exitOptinherit(PostgreSQLParser.OptinheritContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#optpartitionspec}.
	 * @param ctx the parse tree
	 */
	void enterOptpartitionspec(PostgreSQLParser.OptpartitionspecContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#optpartitionspec}.
	 * @param ctx the parse tree
	 */
	void exitOptpartitionspec(PostgreSQLParser.OptpartitionspecContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#partitionspec}.
	 * @param ctx the parse tree
	 */
	void enterPartitionspec(PostgreSQLParser.PartitionspecContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#partitionspec}.
	 * @param ctx the parse tree
	 */
	void exitPartitionspec(PostgreSQLParser.PartitionspecContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#part_params}.
	 * @param ctx the parse tree
	 */
	void enterPart_params(PostgreSQLParser.Part_paramsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#part_params}.
	 * @param ctx the parse tree
	 */
	void exitPart_params(PostgreSQLParser.Part_paramsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#part_elem}.
	 * @param ctx the parse tree
	 */
	void enterPart_elem(PostgreSQLParser.Part_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#part_elem}.
	 * @param ctx the parse tree
	 */
	void exitPart_elem(PostgreSQLParser.Part_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#table_access_method_clause}.
	 * @param ctx the parse tree
	 */
	void enterTable_access_method_clause(PostgreSQLParser.Table_access_method_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#table_access_method_clause}.
	 * @param ctx the parse tree
	 */
	void exitTable_access_method_clause(PostgreSQLParser.Table_access_method_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#optwith}.
	 * @param ctx the parse tree
	 */
	void enterOptwith(PostgreSQLParser.OptwithContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#optwith}.
	 * @param ctx the parse tree
	 */
	void exitOptwith(PostgreSQLParser.OptwithContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#oncommitoption}.
	 * @param ctx the parse tree
	 */
	void enterOncommitoption(PostgreSQLParser.OncommitoptionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#oncommitoption}.
	 * @param ctx the parse tree
	 */
	void exitOncommitoption(PostgreSQLParser.OncommitoptionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#opttablespace}.
	 * @param ctx the parse tree
	 */
	void enterOpttablespace(PostgreSQLParser.OpttablespaceContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#opttablespace}.
	 * @param ctx the parse tree
	 */
	void exitOpttablespace(PostgreSQLParser.OpttablespaceContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#optconstablespace}.
	 * @param ctx the parse tree
	 */
	void enterOptconstablespace(PostgreSQLParser.OptconstablespaceContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#optconstablespace}.
	 * @param ctx the parse tree
	 */
	void exitOptconstablespace(PostgreSQLParser.OptconstablespaceContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#existingindex}.
	 * @param ctx the parse tree
	 */
	void enterExistingindex(PostgreSQLParser.ExistingindexContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#existingindex}.
	 * @param ctx the parse tree
	 */
	void exitExistingindex(PostgreSQLParser.ExistingindexContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createstatsstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreatestatsstmt(PostgreSQLParser.CreatestatsstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createstatsstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreatestatsstmt(PostgreSQLParser.CreatestatsstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterstatsstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterstatsstmt(PostgreSQLParser.AlterstatsstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterstatsstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterstatsstmt(PostgreSQLParser.AlterstatsstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createasstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreateasstmt(PostgreSQLParser.CreateasstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createasstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreateasstmt(PostgreSQLParser.CreateasstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#create_as_target}.
	 * @param ctx the parse tree
	 */
	void enterCreate_as_target(PostgreSQLParser.Create_as_targetContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#create_as_target}.
	 * @param ctx the parse tree
	 */
	void exitCreate_as_target(PostgreSQLParser.Create_as_targetContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#with_data_}.
	 * @param ctx the parse tree
	 */
	void enterWith_data_(PostgreSQLParser.With_data_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#with_data_}.
	 * @param ctx the parse tree
	 */
	void exitWith_data_(PostgreSQLParser.With_data_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#creatematviewstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreatematviewstmt(PostgreSQLParser.CreatematviewstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#creatematviewstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreatematviewstmt(PostgreSQLParser.CreatematviewstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#create_mv_target}.
	 * @param ctx the parse tree
	 */
	void enterCreate_mv_target(PostgreSQLParser.Create_mv_targetContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#create_mv_target}.
	 * @param ctx the parse tree
	 */
	void exitCreate_mv_target(PostgreSQLParser.Create_mv_targetContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#optnolog}.
	 * @param ctx the parse tree
	 */
	void enterOptnolog(PostgreSQLParser.OptnologContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#optnolog}.
	 * @param ctx the parse tree
	 */
	void exitOptnolog(PostgreSQLParser.OptnologContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#refreshmatviewstmt}.
	 * @param ctx the parse tree
	 */
	void enterRefreshmatviewstmt(PostgreSQLParser.RefreshmatviewstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#refreshmatviewstmt}.
	 * @param ctx the parse tree
	 */
	void exitRefreshmatviewstmt(PostgreSQLParser.RefreshmatviewstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createseqstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreateseqstmt(PostgreSQLParser.CreateseqstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createseqstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreateseqstmt(PostgreSQLParser.CreateseqstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterseqstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterseqstmt(PostgreSQLParser.AlterseqstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterseqstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterseqstmt(PostgreSQLParser.AlterseqstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#optseqoptlist}.
	 * @param ctx the parse tree
	 */
	void enterOptseqoptlist(PostgreSQLParser.OptseqoptlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#optseqoptlist}.
	 * @param ctx the parse tree
	 */
	void exitOptseqoptlist(PostgreSQLParser.OptseqoptlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#optparenthesizedseqoptlist}.
	 * @param ctx the parse tree
	 */
	void enterOptparenthesizedseqoptlist(PostgreSQLParser.OptparenthesizedseqoptlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#optparenthesizedseqoptlist}.
	 * @param ctx the parse tree
	 */
	void exitOptparenthesizedseqoptlist(PostgreSQLParser.OptparenthesizedseqoptlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#seqoptlist}.
	 * @param ctx the parse tree
	 */
	void enterSeqoptlist(PostgreSQLParser.SeqoptlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#seqoptlist}.
	 * @param ctx the parse tree
	 */
	void exitSeqoptlist(PostgreSQLParser.SeqoptlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#seqoptelem}.
	 * @param ctx the parse tree
	 */
	void enterSeqoptelem(PostgreSQLParser.SeqoptelemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#seqoptelem}.
	 * @param ctx the parse tree
	 */
	void exitSeqoptelem(PostgreSQLParser.SeqoptelemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#by_}.
	 * @param ctx the parse tree
	 */
	void enterBy_(PostgreSQLParser.By_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#by_}.
	 * @param ctx the parse tree
	 */
	void exitBy_(PostgreSQLParser.By_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#numericonly}.
	 * @param ctx the parse tree
	 */
	void enterNumericonly(PostgreSQLParser.NumericonlyContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#numericonly}.
	 * @param ctx the parse tree
	 */
	void exitNumericonly(PostgreSQLParser.NumericonlyContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#numericonly_list}.
	 * @param ctx the parse tree
	 */
	void enterNumericonly_list(PostgreSQLParser.Numericonly_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#numericonly_list}.
	 * @param ctx the parse tree
	 */
	void exitNumericonly_list(PostgreSQLParser.Numericonly_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createplangstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreateplangstmt(PostgreSQLParser.CreateplangstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createplangstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreateplangstmt(PostgreSQLParser.CreateplangstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#trusted_}.
	 * @param ctx the parse tree
	 */
	void enterTrusted_(PostgreSQLParser.Trusted_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#trusted_}.
	 * @param ctx the parse tree
	 */
	void exitTrusted_(PostgreSQLParser.Trusted_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#handler_name}.
	 * @param ctx the parse tree
	 */
	void enterHandler_name(PostgreSQLParser.Handler_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#handler_name}.
	 * @param ctx the parse tree
	 */
	void exitHandler_name(PostgreSQLParser.Handler_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#inline_handler_}.
	 * @param ctx the parse tree
	 */
	void enterInline_handler_(PostgreSQLParser.Inline_handler_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#inline_handler_}.
	 * @param ctx the parse tree
	 */
	void exitInline_handler_(PostgreSQLParser.Inline_handler_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#validator_clause}.
	 * @param ctx the parse tree
	 */
	void enterValidator_clause(PostgreSQLParser.Validator_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#validator_clause}.
	 * @param ctx the parse tree
	 */
	void exitValidator_clause(PostgreSQLParser.Validator_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#validator_}.
	 * @param ctx the parse tree
	 */
	void enterValidator_(PostgreSQLParser.Validator_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#validator_}.
	 * @param ctx the parse tree
	 */
	void exitValidator_(PostgreSQLParser.Validator_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#procedural_}.
	 * @param ctx the parse tree
	 */
	void enterProcedural_(PostgreSQLParser.Procedural_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#procedural_}.
	 * @param ctx the parse tree
	 */
	void exitProcedural_(PostgreSQLParser.Procedural_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createtablespacestmt}.
	 * @param ctx the parse tree
	 */
	void enterCreatetablespacestmt(PostgreSQLParser.CreatetablespacestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createtablespacestmt}.
	 * @param ctx the parse tree
	 */
	void exitCreatetablespacestmt(PostgreSQLParser.CreatetablespacestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#opttablespaceowner}.
	 * @param ctx the parse tree
	 */
	void enterOpttablespaceowner(PostgreSQLParser.OpttablespaceownerContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#opttablespaceowner}.
	 * @param ctx the parse tree
	 */
	void exitOpttablespaceowner(PostgreSQLParser.OpttablespaceownerContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#droptablespacestmt}.
	 * @param ctx the parse tree
	 */
	void enterDroptablespacestmt(PostgreSQLParser.DroptablespacestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#droptablespacestmt}.
	 * @param ctx the parse tree
	 */
	void exitDroptablespacestmt(PostgreSQLParser.DroptablespacestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createextensionstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreateextensionstmt(PostgreSQLParser.CreateextensionstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createextensionstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreateextensionstmt(PostgreSQLParser.CreateextensionstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#create_extension_opt_list}.
	 * @param ctx the parse tree
	 */
	void enterCreate_extension_opt_list(PostgreSQLParser.Create_extension_opt_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#create_extension_opt_list}.
	 * @param ctx the parse tree
	 */
	void exitCreate_extension_opt_list(PostgreSQLParser.Create_extension_opt_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#create_extension_opt_item}.
	 * @param ctx the parse tree
	 */
	void enterCreate_extension_opt_item(PostgreSQLParser.Create_extension_opt_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#create_extension_opt_item}.
	 * @param ctx the parse tree
	 */
	void exitCreate_extension_opt_item(PostgreSQLParser.Create_extension_opt_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterextensionstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterextensionstmt(PostgreSQLParser.AlterextensionstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterextensionstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterextensionstmt(PostgreSQLParser.AlterextensionstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alter_extension_opt_list}.
	 * @param ctx the parse tree
	 */
	void enterAlter_extension_opt_list(PostgreSQLParser.Alter_extension_opt_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alter_extension_opt_list}.
	 * @param ctx the parse tree
	 */
	void exitAlter_extension_opt_list(PostgreSQLParser.Alter_extension_opt_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alter_extension_opt_item}.
	 * @param ctx the parse tree
	 */
	void enterAlter_extension_opt_item(PostgreSQLParser.Alter_extension_opt_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alter_extension_opt_item}.
	 * @param ctx the parse tree
	 */
	void exitAlter_extension_opt_item(PostgreSQLParser.Alter_extension_opt_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterextensioncontentsstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterextensioncontentsstmt(PostgreSQLParser.AlterextensioncontentsstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterextensioncontentsstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterextensioncontentsstmt(PostgreSQLParser.AlterextensioncontentsstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createfdwstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreatefdwstmt(PostgreSQLParser.CreatefdwstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createfdwstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreatefdwstmt(PostgreSQLParser.CreatefdwstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#fdw_option}.
	 * @param ctx the parse tree
	 */
	void enterFdw_option(PostgreSQLParser.Fdw_optionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#fdw_option}.
	 * @param ctx the parse tree
	 */
	void exitFdw_option(PostgreSQLParser.Fdw_optionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#fdw_options}.
	 * @param ctx the parse tree
	 */
	void enterFdw_options(PostgreSQLParser.Fdw_optionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#fdw_options}.
	 * @param ctx the parse tree
	 */
	void exitFdw_options(PostgreSQLParser.Fdw_optionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#fdw_options_}.
	 * @param ctx the parse tree
	 */
	void enterFdw_options_(PostgreSQLParser.Fdw_options_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#fdw_options_}.
	 * @param ctx the parse tree
	 */
	void exitFdw_options_(PostgreSQLParser.Fdw_options_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterfdwstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterfdwstmt(PostgreSQLParser.AlterfdwstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterfdwstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterfdwstmt(PostgreSQLParser.AlterfdwstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#create_generic_options}.
	 * @param ctx the parse tree
	 */
	void enterCreate_generic_options(PostgreSQLParser.Create_generic_optionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#create_generic_options}.
	 * @param ctx the parse tree
	 */
	void exitCreate_generic_options(PostgreSQLParser.Create_generic_optionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#generic_option_list}.
	 * @param ctx the parse tree
	 */
	void enterGeneric_option_list(PostgreSQLParser.Generic_option_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#generic_option_list}.
	 * @param ctx the parse tree
	 */
	void exitGeneric_option_list(PostgreSQLParser.Generic_option_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alter_generic_options}.
	 * @param ctx the parse tree
	 */
	void enterAlter_generic_options(PostgreSQLParser.Alter_generic_optionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alter_generic_options}.
	 * @param ctx the parse tree
	 */
	void exitAlter_generic_options(PostgreSQLParser.Alter_generic_optionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alter_generic_option_list}.
	 * @param ctx the parse tree
	 */
	void enterAlter_generic_option_list(PostgreSQLParser.Alter_generic_option_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alter_generic_option_list}.
	 * @param ctx the parse tree
	 */
	void exitAlter_generic_option_list(PostgreSQLParser.Alter_generic_option_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alter_generic_option_elem}.
	 * @param ctx the parse tree
	 */
	void enterAlter_generic_option_elem(PostgreSQLParser.Alter_generic_option_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alter_generic_option_elem}.
	 * @param ctx the parse tree
	 */
	void exitAlter_generic_option_elem(PostgreSQLParser.Alter_generic_option_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#generic_option_elem}.
	 * @param ctx the parse tree
	 */
	void enterGeneric_option_elem(PostgreSQLParser.Generic_option_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#generic_option_elem}.
	 * @param ctx the parse tree
	 */
	void exitGeneric_option_elem(PostgreSQLParser.Generic_option_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#generic_option_name}.
	 * @param ctx the parse tree
	 */
	void enterGeneric_option_name(PostgreSQLParser.Generic_option_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#generic_option_name}.
	 * @param ctx the parse tree
	 */
	void exitGeneric_option_name(PostgreSQLParser.Generic_option_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#generic_option_arg}.
	 * @param ctx the parse tree
	 */
	void enterGeneric_option_arg(PostgreSQLParser.Generic_option_argContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#generic_option_arg}.
	 * @param ctx the parse tree
	 */
	void exitGeneric_option_arg(PostgreSQLParser.Generic_option_argContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createforeignserverstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreateforeignserverstmt(PostgreSQLParser.CreateforeignserverstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createforeignserverstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreateforeignserverstmt(PostgreSQLParser.CreateforeignserverstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#type_}.
	 * @param ctx the parse tree
	 */
	void enterType_(PostgreSQLParser.Type_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#type_}.
	 * @param ctx the parse tree
	 */
	void exitType_(PostgreSQLParser.Type_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#foreign_server_version}.
	 * @param ctx the parse tree
	 */
	void enterForeign_server_version(PostgreSQLParser.Foreign_server_versionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#foreign_server_version}.
	 * @param ctx the parse tree
	 */
	void exitForeign_server_version(PostgreSQLParser.Foreign_server_versionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#foreign_server_version_}.
	 * @param ctx the parse tree
	 */
	void enterForeign_server_version_(PostgreSQLParser.Foreign_server_version_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#foreign_server_version_}.
	 * @param ctx the parse tree
	 */
	void exitForeign_server_version_(PostgreSQLParser.Foreign_server_version_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterforeignserverstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterforeignserverstmt(PostgreSQLParser.AlterforeignserverstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterforeignserverstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterforeignserverstmt(PostgreSQLParser.AlterforeignserverstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createforeigntablestmt}.
	 * @param ctx the parse tree
	 */
	void enterCreateforeigntablestmt(PostgreSQLParser.CreateforeigntablestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createforeigntablestmt}.
	 * @param ctx the parse tree
	 */
	void exitCreateforeigntablestmt(PostgreSQLParser.CreateforeigntablestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#importforeignschemastmt}.
	 * @param ctx the parse tree
	 */
	void enterImportforeignschemastmt(PostgreSQLParser.ImportforeignschemastmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#importforeignschemastmt}.
	 * @param ctx the parse tree
	 */
	void exitImportforeignschemastmt(PostgreSQLParser.ImportforeignschemastmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#import_qualification_type}.
	 * @param ctx the parse tree
	 */
	void enterImport_qualification_type(PostgreSQLParser.Import_qualification_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#import_qualification_type}.
	 * @param ctx the parse tree
	 */
	void exitImport_qualification_type(PostgreSQLParser.Import_qualification_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#import_qualification}.
	 * @param ctx the parse tree
	 */
	void enterImport_qualification(PostgreSQLParser.Import_qualificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#import_qualification}.
	 * @param ctx the parse tree
	 */
	void exitImport_qualification(PostgreSQLParser.Import_qualificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createusermappingstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreateusermappingstmt(PostgreSQLParser.CreateusermappingstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createusermappingstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreateusermappingstmt(PostgreSQLParser.CreateusermappingstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#auth_ident}.
	 * @param ctx the parse tree
	 */
	void enterAuth_ident(PostgreSQLParser.Auth_identContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#auth_ident}.
	 * @param ctx the parse tree
	 */
	void exitAuth_ident(PostgreSQLParser.Auth_identContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#dropusermappingstmt}.
	 * @param ctx the parse tree
	 */
	void enterDropusermappingstmt(PostgreSQLParser.DropusermappingstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#dropusermappingstmt}.
	 * @param ctx the parse tree
	 */
	void exitDropusermappingstmt(PostgreSQLParser.DropusermappingstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterusermappingstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterusermappingstmt(PostgreSQLParser.AlterusermappingstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterusermappingstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterusermappingstmt(PostgreSQLParser.AlterusermappingstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createpolicystmt}.
	 * @param ctx the parse tree
	 */
	void enterCreatepolicystmt(PostgreSQLParser.CreatepolicystmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createpolicystmt}.
	 * @param ctx the parse tree
	 */
	void exitCreatepolicystmt(PostgreSQLParser.CreatepolicystmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterpolicystmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterpolicystmt(PostgreSQLParser.AlterpolicystmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterpolicystmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterpolicystmt(PostgreSQLParser.AlterpolicystmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#rowsecurityoptionalexpr}.
	 * @param ctx the parse tree
	 */
	void enterRowsecurityoptionalexpr(PostgreSQLParser.RowsecurityoptionalexprContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#rowsecurityoptionalexpr}.
	 * @param ctx the parse tree
	 */
	void exitRowsecurityoptionalexpr(PostgreSQLParser.RowsecurityoptionalexprContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#rowsecurityoptionalwithcheck}.
	 * @param ctx the parse tree
	 */
	void enterRowsecurityoptionalwithcheck(PostgreSQLParser.RowsecurityoptionalwithcheckContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#rowsecurityoptionalwithcheck}.
	 * @param ctx the parse tree
	 */
	void exitRowsecurityoptionalwithcheck(PostgreSQLParser.RowsecurityoptionalwithcheckContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#rowsecuritydefaulttorole}.
	 * @param ctx the parse tree
	 */
	void enterRowsecuritydefaulttorole(PostgreSQLParser.RowsecuritydefaulttoroleContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#rowsecuritydefaulttorole}.
	 * @param ctx the parse tree
	 */
	void exitRowsecuritydefaulttorole(PostgreSQLParser.RowsecuritydefaulttoroleContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#rowsecurityoptionaltorole}.
	 * @param ctx the parse tree
	 */
	void enterRowsecurityoptionaltorole(PostgreSQLParser.RowsecurityoptionaltoroleContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#rowsecurityoptionaltorole}.
	 * @param ctx the parse tree
	 */
	void exitRowsecurityoptionaltorole(PostgreSQLParser.RowsecurityoptionaltoroleContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#rowsecuritydefaultpermissive}.
	 * @param ctx the parse tree
	 */
	void enterRowsecuritydefaultpermissive(PostgreSQLParser.RowsecuritydefaultpermissiveContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#rowsecuritydefaultpermissive}.
	 * @param ctx the parse tree
	 */
	void exitRowsecuritydefaultpermissive(PostgreSQLParser.RowsecuritydefaultpermissiveContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#rowsecuritydefaultforcmd}.
	 * @param ctx the parse tree
	 */
	void enterRowsecuritydefaultforcmd(PostgreSQLParser.RowsecuritydefaultforcmdContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#rowsecuritydefaultforcmd}.
	 * @param ctx the parse tree
	 */
	void exitRowsecuritydefaultforcmd(PostgreSQLParser.RowsecuritydefaultforcmdContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#row_security_cmd}.
	 * @param ctx the parse tree
	 */
	void enterRow_security_cmd(PostgreSQLParser.Row_security_cmdContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#row_security_cmd}.
	 * @param ctx the parse tree
	 */
	void exitRow_security_cmd(PostgreSQLParser.Row_security_cmdContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createamstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreateamstmt(PostgreSQLParser.CreateamstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createamstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreateamstmt(PostgreSQLParser.CreateamstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#am_type}.
	 * @param ctx the parse tree
	 */
	void enterAm_type(PostgreSQLParser.Am_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#am_type}.
	 * @param ctx the parse tree
	 */
	void exitAm_type(PostgreSQLParser.Am_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createtrigstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreatetrigstmt(PostgreSQLParser.CreatetrigstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createtrigstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreatetrigstmt(PostgreSQLParser.CreatetrigstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#triggeractiontime}.
	 * @param ctx the parse tree
	 */
	void enterTriggeractiontime(PostgreSQLParser.TriggeractiontimeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#triggeractiontime}.
	 * @param ctx the parse tree
	 */
	void exitTriggeractiontime(PostgreSQLParser.TriggeractiontimeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#triggerevents}.
	 * @param ctx the parse tree
	 */
	void enterTriggerevents(PostgreSQLParser.TriggereventsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#triggerevents}.
	 * @param ctx the parse tree
	 */
	void exitTriggerevents(PostgreSQLParser.TriggereventsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#triggeroneevent}.
	 * @param ctx the parse tree
	 */
	void enterTriggeroneevent(PostgreSQLParser.TriggeroneeventContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#triggeroneevent}.
	 * @param ctx the parse tree
	 */
	void exitTriggeroneevent(PostgreSQLParser.TriggeroneeventContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#triggerreferencing}.
	 * @param ctx the parse tree
	 */
	void enterTriggerreferencing(PostgreSQLParser.TriggerreferencingContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#triggerreferencing}.
	 * @param ctx the parse tree
	 */
	void exitTriggerreferencing(PostgreSQLParser.TriggerreferencingContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#triggertransitions}.
	 * @param ctx the parse tree
	 */
	void enterTriggertransitions(PostgreSQLParser.TriggertransitionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#triggertransitions}.
	 * @param ctx the parse tree
	 */
	void exitTriggertransitions(PostgreSQLParser.TriggertransitionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#triggertransition}.
	 * @param ctx the parse tree
	 */
	void enterTriggertransition(PostgreSQLParser.TriggertransitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#triggertransition}.
	 * @param ctx the parse tree
	 */
	void exitTriggertransition(PostgreSQLParser.TriggertransitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#transitionoldornew}.
	 * @param ctx the parse tree
	 */
	void enterTransitionoldornew(PostgreSQLParser.TransitionoldornewContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#transitionoldornew}.
	 * @param ctx the parse tree
	 */
	void exitTransitionoldornew(PostgreSQLParser.TransitionoldornewContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#transitionrowortable}.
	 * @param ctx the parse tree
	 */
	void enterTransitionrowortable(PostgreSQLParser.TransitionrowortableContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#transitionrowortable}.
	 * @param ctx the parse tree
	 */
	void exitTransitionrowortable(PostgreSQLParser.TransitionrowortableContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#transitionrelname}.
	 * @param ctx the parse tree
	 */
	void enterTransitionrelname(PostgreSQLParser.TransitionrelnameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#transitionrelname}.
	 * @param ctx the parse tree
	 */
	void exitTransitionrelname(PostgreSQLParser.TransitionrelnameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#triggerforspec}.
	 * @param ctx the parse tree
	 */
	void enterTriggerforspec(PostgreSQLParser.TriggerforspecContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#triggerforspec}.
	 * @param ctx the parse tree
	 */
	void exitTriggerforspec(PostgreSQLParser.TriggerforspecContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#triggerforopteach}.
	 * @param ctx the parse tree
	 */
	void enterTriggerforopteach(PostgreSQLParser.TriggerforopteachContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#triggerforopteach}.
	 * @param ctx the parse tree
	 */
	void exitTriggerforopteach(PostgreSQLParser.TriggerforopteachContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#triggerfortype}.
	 * @param ctx the parse tree
	 */
	void enterTriggerfortype(PostgreSQLParser.TriggerfortypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#triggerfortype}.
	 * @param ctx the parse tree
	 */
	void exitTriggerfortype(PostgreSQLParser.TriggerfortypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#triggerwhen}.
	 * @param ctx the parse tree
	 */
	void enterTriggerwhen(PostgreSQLParser.TriggerwhenContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#triggerwhen}.
	 * @param ctx the parse tree
	 */
	void exitTriggerwhen(PostgreSQLParser.TriggerwhenContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#function_or_procedure}.
	 * @param ctx the parse tree
	 */
	void enterFunction_or_procedure(PostgreSQLParser.Function_or_procedureContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#function_or_procedure}.
	 * @param ctx the parse tree
	 */
	void exitFunction_or_procedure(PostgreSQLParser.Function_or_procedureContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#triggerfuncargs}.
	 * @param ctx the parse tree
	 */
	void enterTriggerfuncargs(PostgreSQLParser.TriggerfuncargsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#triggerfuncargs}.
	 * @param ctx the parse tree
	 */
	void exitTriggerfuncargs(PostgreSQLParser.TriggerfuncargsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#triggerfuncarg}.
	 * @param ctx the parse tree
	 */
	void enterTriggerfuncarg(PostgreSQLParser.TriggerfuncargContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#triggerfuncarg}.
	 * @param ctx the parse tree
	 */
	void exitTriggerfuncarg(PostgreSQLParser.TriggerfuncargContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#optconstrfromtable}.
	 * @param ctx the parse tree
	 */
	void enterOptconstrfromtable(PostgreSQLParser.OptconstrfromtableContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#optconstrfromtable}.
	 * @param ctx the parse tree
	 */
	void exitOptconstrfromtable(PostgreSQLParser.OptconstrfromtableContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#constraintattributespec}.
	 * @param ctx the parse tree
	 */
	void enterConstraintattributespec(PostgreSQLParser.ConstraintattributespecContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#constraintattributespec}.
	 * @param ctx the parse tree
	 */
	void exitConstraintattributespec(PostgreSQLParser.ConstraintattributespecContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#constraintattributeElem}.
	 * @param ctx the parse tree
	 */
	void enterConstraintattributeElem(PostgreSQLParser.ConstraintattributeElemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#constraintattributeElem}.
	 * @param ctx the parse tree
	 */
	void exitConstraintattributeElem(PostgreSQLParser.ConstraintattributeElemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createeventtrigstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreateeventtrigstmt(PostgreSQLParser.CreateeventtrigstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createeventtrigstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreateeventtrigstmt(PostgreSQLParser.CreateeventtrigstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#event_trigger_when_list}.
	 * @param ctx the parse tree
	 */
	void enterEvent_trigger_when_list(PostgreSQLParser.Event_trigger_when_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#event_trigger_when_list}.
	 * @param ctx the parse tree
	 */
	void exitEvent_trigger_when_list(PostgreSQLParser.Event_trigger_when_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#event_trigger_when_item}.
	 * @param ctx the parse tree
	 */
	void enterEvent_trigger_when_item(PostgreSQLParser.Event_trigger_when_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#event_trigger_when_item}.
	 * @param ctx the parse tree
	 */
	void exitEvent_trigger_when_item(PostgreSQLParser.Event_trigger_when_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#event_trigger_value_list}.
	 * @param ctx the parse tree
	 */
	void enterEvent_trigger_value_list(PostgreSQLParser.Event_trigger_value_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#event_trigger_value_list}.
	 * @param ctx the parse tree
	 */
	void exitEvent_trigger_value_list(PostgreSQLParser.Event_trigger_value_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#altereventtrigstmt}.
	 * @param ctx the parse tree
	 */
	void enterAltereventtrigstmt(PostgreSQLParser.AltereventtrigstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#altereventtrigstmt}.
	 * @param ctx the parse tree
	 */
	void exitAltereventtrigstmt(PostgreSQLParser.AltereventtrigstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#enable_trigger}.
	 * @param ctx the parse tree
	 */
	void enterEnable_trigger(PostgreSQLParser.Enable_triggerContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#enable_trigger}.
	 * @param ctx the parse tree
	 */
	void exitEnable_trigger(PostgreSQLParser.Enable_triggerContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createassertionstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreateassertionstmt(PostgreSQLParser.CreateassertionstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createassertionstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreateassertionstmt(PostgreSQLParser.CreateassertionstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#definestmt}.
	 * @param ctx the parse tree
	 */
	void enterDefinestmt(PostgreSQLParser.DefinestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#definestmt}.
	 * @param ctx the parse tree
	 */
	void exitDefinestmt(PostgreSQLParser.DefinestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#definition}.
	 * @param ctx the parse tree
	 */
	void enterDefinition(PostgreSQLParser.DefinitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#definition}.
	 * @param ctx the parse tree
	 */
	void exitDefinition(PostgreSQLParser.DefinitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#def_list}.
	 * @param ctx the parse tree
	 */
	void enterDef_list(PostgreSQLParser.Def_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#def_list}.
	 * @param ctx the parse tree
	 */
	void exitDef_list(PostgreSQLParser.Def_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#def_elem}.
	 * @param ctx the parse tree
	 */
	void enterDef_elem(PostgreSQLParser.Def_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#def_elem}.
	 * @param ctx the parse tree
	 */
	void exitDef_elem(PostgreSQLParser.Def_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#def_arg}.
	 * @param ctx the parse tree
	 */
	void enterDef_arg(PostgreSQLParser.Def_argContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#def_arg}.
	 * @param ctx the parse tree
	 */
	void exitDef_arg(PostgreSQLParser.Def_argContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#old_aggr_definition}.
	 * @param ctx the parse tree
	 */
	void enterOld_aggr_definition(PostgreSQLParser.Old_aggr_definitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#old_aggr_definition}.
	 * @param ctx the parse tree
	 */
	void exitOld_aggr_definition(PostgreSQLParser.Old_aggr_definitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#old_aggr_list}.
	 * @param ctx the parse tree
	 */
	void enterOld_aggr_list(PostgreSQLParser.Old_aggr_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#old_aggr_list}.
	 * @param ctx the parse tree
	 */
	void exitOld_aggr_list(PostgreSQLParser.Old_aggr_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#old_aggr_elem}.
	 * @param ctx the parse tree
	 */
	void enterOld_aggr_elem(PostgreSQLParser.Old_aggr_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#old_aggr_elem}.
	 * @param ctx the parse tree
	 */
	void exitOld_aggr_elem(PostgreSQLParser.Old_aggr_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#enum_val_list_}.
	 * @param ctx the parse tree
	 */
	void enterEnum_val_list_(PostgreSQLParser.Enum_val_list_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#enum_val_list_}.
	 * @param ctx the parse tree
	 */
	void exitEnum_val_list_(PostgreSQLParser.Enum_val_list_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#enum_val_list}.
	 * @param ctx the parse tree
	 */
	void enterEnum_val_list(PostgreSQLParser.Enum_val_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#enum_val_list}.
	 * @param ctx the parse tree
	 */
	void exitEnum_val_list(PostgreSQLParser.Enum_val_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterenumstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterenumstmt(PostgreSQLParser.AlterenumstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterenumstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterenumstmt(PostgreSQLParser.AlterenumstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#if_not_exists_}.
	 * @param ctx the parse tree
	 */
	void enterIf_not_exists_(PostgreSQLParser.If_not_exists_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#if_not_exists_}.
	 * @param ctx the parse tree
	 */
	void exitIf_not_exists_(PostgreSQLParser.If_not_exists_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createopclassstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreateopclassstmt(PostgreSQLParser.CreateopclassstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createopclassstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreateopclassstmt(PostgreSQLParser.CreateopclassstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#opclass_item_list}.
	 * @param ctx the parse tree
	 */
	void enterOpclass_item_list(PostgreSQLParser.Opclass_item_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#opclass_item_list}.
	 * @param ctx the parse tree
	 */
	void exitOpclass_item_list(PostgreSQLParser.Opclass_item_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#opclass_item}.
	 * @param ctx the parse tree
	 */
	void enterOpclass_item(PostgreSQLParser.Opclass_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#opclass_item}.
	 * @param ctx the parse tree
	 */
	void exitOpclass_item(PostgreSQLParser.Opclass_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#default_}.
	 * @param ctx the parse tree
	 */
	void enterDefault_(PostgreSQLParser.Default_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#default_}.
	 * @param ctx the parse tree
	 */
	void exitDefault_(PostgreSQLParser.Default_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#opfamily_}.
	 * @param ctx the parse tree
	 */
	void enterOpfamily_(PostgreSQLParser.Opfamily_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#opfamily_}.
	 * @param ctx the parse tree
	 */
	void exitOpfamily_(PostgreSQLParser.Opfamily_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#opclass_purpose}.
	 * @param ctx the parse tree
	 */
	void enterOpclass_purpose(PostgreSQLParser.Opclass_purposeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#opclass_purpose}.
	 * @param ctx the parse tree
	 */
	void exitOpclass_purpose(PostgreSQLParser.Opclass_purposeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#recheck_}.
	 * @param ctx the parse tree
	 */
	void enterRecheck_(PostgreSQLParser.Recheck_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#recheck_}.
	 * @param ctx the parse tree
	 */
	void exitRecheck_(PostgreSQLParser.Recheck_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createopfamilystmt}.
	 * @param ctx the parse tree
	 */
	void enterCreateopfamilystmt(PostgreSQLParser.CreateopfamilystmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createopfamilystmt}.
	 * @param ctx the parse tree
	 */
	void exitCreateopfamilystmt(PostgreSQLParser.CreateopfamilystmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alteropfamilystmt}.
	 * @param ctx the parse tree
	 */
	void enterAlteropfamilystmt(PostgreSQLParser.AlteropfamilystmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alteropfamilystmt}.
	 * @param ctx the parse tree
	 */
	void exitAlteropfamilystmt(PostgreSQLParser.AlteropfamilystmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#opclass_drop_list}.
	 * @param ctx the parse tree
	 */
	void enterOpclass_drop_list(PostgreSQLParser.Opclass_drop_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#opclass_drop_list}.
	 * @param ctx the parse tree
	 */
	void exitOpclass_drop_list(PostgreSQLParser.Opclass_drop_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#opclass_drop}.
	 * @param ctx the parse tree
	 */
	void enterOpclass_drop(PostgreSQLParser.Opclass_dropContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#opclass_drop}.
	 * @param ctx the parse tree
	 */
	void exitOpclass_drop(PostgreSQLParser.Opclass_dropContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#dropopclassstmt}.
	 * @param ctx the parse tree
	 */
	void enterDropopclassstmt(PostgreSQLParser.DropopclassstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#dropopclassstmt}.
	 * @param ctx the parse tree
	 */
	void exitDropopclassstmt(PostgreSQLParser.DropopclassstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#dropopfamilystmt}.
	 * @param ctx the parse tree
	 */
	void enterDropopfamilystmt(PostgreSQLParser.DropopfamilystmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#dropopfamilystmt}.
	 * @param ctx the parse tree
	 */
	void exitDropopfamilystmt(PostgreSQLParser.DropopfamilystmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#dropownedstmt}.
	 * @param ctx the parse tree
	 */
	void enterDropownedstmt(PostgreSQLParser.DropownedstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#dropownedstmt}.
	 * @param ctx the parse tree
	 */
	void exitDropownedstmt(PostgreSQLParser.DropownedstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#reassignownedstmt}.
	 * @param ctx the parse tree
	 */
	void enterReassignownedstmt(PostgreSQLParser.ReassignownedstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#reassignownedstmt}.
	 * @param ctx the parse tree
	 */
	void exitReassignownedstmt(PostgreSQLParser.ReassignownedstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#dropstmt}.
	 * @param ctx the parse tree
	 */
	void enterDropstmt(PostgreSQLParser.DropstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#dropstmt}.
	 * @param ctx the parse tree
	 */
	void exitDropstmt(PostgreSQLParser.DropstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#object_type_any_name}.
	 * @param ctx the parse tree
	 */
	void enterObject_type_any_name(PostgreSQLParser.Object_type_any_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#object_type_any_name}.
	 * @param ctx the parse tree
	 */
	void exitObject_type_any_name(PostgreSQLParser.Object_type_any_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#object_type_name}.
	 * @param ctx the parse tree
	 */
	void enterObject_type_name(PostgreSQLParser.Object_type_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#object_type_name}.
	 * @param ctx the parse tree
	 */
	void exitObject_type_name(PostgreSQLParser.Object_type_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#drop_type_name}.
	 * @param ctx the parse tree
	 */
	void enterDrop_type_name(PostgreSQLParser.Drop_type_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#drop_type_name}.
	 * @param ctx the parse tree
	 */
	void exitDrop_type_name(PostgreSQLParser.Drop_type_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#object_type_name_on_any_name}.
	 * @param ctx the parse tree
	 */
	void enterObject_type_name_on_any_name(PostgreSQLParser.Object_type_name_on_any_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#object_type_name_on_any_name}.
	 * @param ctx the parse tree
	 */
	void exitObject_type_name_on_any_name(PostgreSQLParser.Object_type_name_on_any_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#any_name_list_}.
	 * @param ctx the parse tree
	 */
	void enterAny_name_list_(PostgreSQLParser.Any_name_list_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#any_name_list_}.
	 * @param ctx the parse tree
	 */
	void exitAny_name_list_(PostgreSQLParser.Any_name_list_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#any_name}.
	 * @param ctx the parse tree
	 */
	void enterAny_name(PostgreSQLParser.Any_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#any_name}.
	 * @param ctx the parse tree
	 */
	void exitAny_name(PostgreSQLParser.Any_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#attrs}.
	 * @param ctx the parse tree
	 */
	void enterAttrs(PostgreSQLParser.AttrsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#attrs}.
	 * @param ctx the parse tree
	 */
	void exitAttrs(PostgreSQLParser.AttrsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#type_name_list}.
	 * @param ctx the parse tree
	 */
	void enterType_name_list(PostgreSQLParser.Type_name_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#type_name_list}.
	 * @param ctx the parse tree
	 */
	void exitType_name_list(PostgreSQLParser.Type_name_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#truncatestmt}.
	 * @param ctx the parse tree
	 */
	void enterTruncatestmt(PostgreSQLParser.TruncatestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#truncatestmt}.
	 * @param ctx the parse tree
	 */
	void exitTruncatestmt(PostgreSQLParser.TruncatestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#restart_seqs_}.
	 * @param ctx the parse tree
	 */
	void enterRestart_seqs_(PostgreSQLParser.Restart_seqs_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#restart_seqs_}.
	 * @param ctx the parse tree
	 */
	void exitRestart_seqs_(PostgreSQLParser.Restart_seqs_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#commentstmt}.
	 * @param ctx the parse tree
	 */
	void enterCommentstmt(PostgreSQLParser.CommentstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#commentstmt}.
	 * @param ctx the parse tree
	 */
	void exitCommentstmt(PostgreSQLParser.CommentstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#comment_text}.
	 * @param ctx the parse tree
	 */
	void enterComment_text(PostgreSQLParser.Comment_textContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#comment_text}.
	 * @param ctx the parse tree
	 */
	void exitComment_text(PostgreSQLParser.Comment_textContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#seclabelstmt}.
	 * @param ctx the parse tree
	 */
	void enterSeclabelstmt(PostgreSQLParser.SeclabelstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#seclabelstmt}.
	 * @param ctx the parse tree
	 */
	void exitSeclabelstmt(PostgreSQLParser.SeclabelstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#provider_}.
	 * @param ctx the parse tree
	 */
	void enterProvider_(PostgreSQLParser.Provider_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#provider_}.
	 * @param ctx the parse tree
	 */
	void exitProvider_(PostgreSQLParser.Provider_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#security_label}.
	 * @param ctx the parse tree
	 */
	void enterSecurity_label(PostgreSQLParser.Security_labelContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#security_label}.
	 * @param ctx the parse tree
	 */
	void exitSecurity_label(PostgreSQLParser.Security_labelContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#fetchstmt}.
	 * @param ctx the parse tree
	 */
	void enterFetchstmt(PostgreSQLParser.FetchstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#fetchstmt}.
	 * @param ctx the parse tree
	 */
	void exitFetchstmt(PostgreSQLParser.FetchstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#fetch_args}.
	 * @param ctx the parse tree
	 */
	void enterFetch_args(PostgreSQLParser.Fetch_argsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#fetch_args}.
	 * @param ctx the parse tree
	 */
	void exitFetch_args(PostgreSQLParser.Fetch_argsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#from_in}.
	 * @param ctx the parse tree
	 */
	void enterFrom_in(PostgreSQLParser.From_inContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#from_in}.
	 * @param ctx the parse tree
	 */
	void exitFrom_in(PostgreSQLParser.From_inContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#from_in_}.
	 * @param ctx the parse tree
	 */
	void enterFrom_in_(PostgreSQLParser.From_in_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#from_in_}.
	 * @param ctx the parse tree
	 */
	void exitFrom_in_(PostgreSQLParser.From_in_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#grantstmt}.
	 * @param ctx the parse tree
	 */
	void enterGrantstmt(PostgreSQLParser.GrantstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#grantstmt}.
	 * @param ctx the parse tree
	 */
	void exitGrantstmt(PostgreSQLParser.GrantstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#revokestmt}.
	 * @param ctx the parse tree
	 */
	void enterRevokestmt(PostgreSQLParser.RevokestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#revokestmt}.
	 * @param ctx the parse tree
	 */
	void exitRevokestmt(PostgreSQLParser.RevokestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#privileges}.
	 * @param ctx the parse tree
	 */
	void enterPrivileges(PostgreSQLParser.PrivilegesContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#privileges}.
	 * @param ctx the parse tree
	 */
	void exitPrivileges(PostgreSQLParser.PrivilegesContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#privilege_list}.
	 * @param ctx the parse tree
	 */
	void enterPrivilege_list(PostgreSQLParser.Privilege_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#privilege_list}.
	 * @param ctx the parse tree
	 */
	void exitPrivilege_list(PostgreSQLParser.Privilege_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#privilege}.
	 * @param ctx the parse tree
	 */
	void enterPrivilege(PostgreSQLParser.PrivilegeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#privilege}.
	 * @param ctx the parse tree
	 */
	void exitPrivilege(PostgreSQLParser.PrivilegeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#privilege_target}.
	 * @param ctx the parse tree
	 */
	void enterPrivilege_target(PostgreSQLParser.Privilege_targetContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#privilege_target}.
	 * @param ctx the parse tree
	 */
	void exitPrivilege_target(PostgreSQLParser.Privilege_targetContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#grantee_list}.
	 * @param ctx the parse tree
	 */
	void enterGrantee_list(PostgreSQLParser.Grantee_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#grantee_list}.
	 * @param ctx the parse tree
	 */
	void exitGrantee_list(PostgreSQLParser.Grantee_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#grantee}.
	 * @param ctx the parse tree
	 */
	void enterGrantee(PostgreSQLParser.GranteeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#grantee}.
	 * @param ctx the parse tree
	 */
	void exitGrantee(PostgreSQLParser.GranteeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#grant_grant_option_}.
	 * @param ctx the parse tree
	 */
	void enterGrant_grant_option_(PostgreSQLParser.Grant_grant_option_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#grant_grant_option_}.
	 * @param ctx the parse tree
	 */
	void exitGrant_grant_option_(PostgreSQLParser.Grant_grant_option_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#grantrolestmt}.
	 * @param ctx the parse tree
	 */
	void enterGrantrolestmt(PostgreSQLParser.GrantrolestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#grantrolestmt}.
	 * @param ctx the parse tree
	 */
	void exitGrantrolestmt(PostgreSQLParser.GrantrolestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#revokerolestmt}.
	 * @param ctx the parse tree
	 */
	void enterRevokerolestmt(PostgreSQLParser.RevokerolestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#revokerolestmt}.
	 * @param ctx the parse tree
	 */
	void exitRevokerolestmt(PostgreSQLParser.RevokerolestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#grant_admin_option_}.
	 * @param ctx the parse tree
	 */
	void enterGrant_admin_option_(PostgreSQLParser.Grant_admin_option_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#grant_admin_option_}.
	 * @param ctx the parse tree
	 */
	void exitGrant_admin_option_(PostgreSQLParser.Grant_admin_option_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#granted_by_}.
	 * @param ctx the parse tree
	 */
	void enterGranted_by_(PostgreSQLParser.Granted_by_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#granted_by_}.
	 * @param ctx the parse tree
	 */
	void exitGranted_by_(PostgreSQLParser.Granted_by_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterdefaultprivilegesstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterdefaultprivilegesstmt(PostgreSQLParser.AlterdefaultprivilegesstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterdefaultprivilegesstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterdefaultprivilegesstmt(PostgreSQLParser.AlterdefaultprivilegesstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#defacloptionlist}.
	 * @param ctx the parse tree
	 */
	void enterDefacloptionlist(PostgreSQLParser.DefacloptionlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#defacloptionlist}.
	 * @param ctx the parse tree
	 */
	void exitDefacloptionlist(PostgreSQLParser.DefacloptionlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#defacloption}.
	 * @param ctx the parse tree
	 */
	void enterDefacloption(PostgreSQLParser.DefacloptionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#defacloption}.
	 * @param ctx the parse tree
	 */
	void exitDefacloption(PostgreSQLParser.DefacloptionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#defaclaction}.
	 * @param ctx the parse tree
	 */
	void enterDefaclaction(PostgreSQLParser.DefaclactionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#defaclaction}.
	 * @param ctx the parse tree
	 */
	void exitDefaclaction(PostgreSQLParser.DefaclactionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#defacl_privilege_target}.
	 * @param ctx the parse tree
	 */
	void enterDefacl_privilege_target(PostgreSQLParser.Defacl_privilege_targetContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#defacl_privilege_target}.
	 * @param ctx the parse tree
	 */
	void exitDefacl_privilege_target(PostgreSQLParser.Defacl_privilege_targetContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#indexstmt}.
	 * @param ctx the parse tree
	 */
	void enterIndexstmt(PostgreSQLParser.IndexstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#indexstmt}.
	 * @param ctx the parse tree
	 */
	void exitIndexstmt(PostgreSQLParser.IndexstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#unique_}.
	 * @param ctx the parse tree
	 */
	void enterUnique_(PostgreSQLParser.Unique_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#unique_}.
	 * @param ctx the parse tree
	 */
	void exitUnique_(PostgreSQLParser.Unique_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#single_name_}.
	 * @param ctx the parse tree
	 */
	void enterSingle_name_(PostgreSQLParser.Single_name_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#single_name_}.
	 * @param ctx the parse tree
	 */
	void exitSingle_name_(PostgreSQLParser.Single_name_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#concurrently_}.
	 * @param ctx the parse tree
	 */
	void enterConcurrently_(PostgreSQLParser.Concurrently_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#concurrently_}.
	 * @param ctx the parse tree
	 */
	void exitConcurrently_(PostgreSQLParser.Concurrently_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#index_name_}.
	 * @param ctx the parse tree
	 */
	void enterIndex_name_(PostgreSQLParser.Index_name_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#index_name_}.
	 * @param ctx the parse tree
	 */
	void exitIndex_name_(PostgreSQLParser.Index_name_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#access_method_clause}.
	 * @param ctx the parse tree
	 */
	void enterAccess_method_clause(PostgreSQLParser.Access_method_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#access_method_clause}.
	 * @param ctx the parse tree
	 */
	void exitAccess_method_clause(PostgreSQLParser.Access_method_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#index_params}.
	 * @param ctx the parse tree
	 */
	void enterIndex_params(PostgreSQLParser.Index_paramsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#index_params}.
	 * @param ctx the parse tree
	 */
	void exitIndex_params(PostgreSQLParser.Index_paramsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#index_elem_options}.
	 * @param ctx the parse tree
	 */
	void enterIndex_elem_options(PostgreSQLParser.Index_elem_optionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#index_elem_options}.
	 * @param ctx the parse tree
	 */
	void exitIndex_elem_options(PostgreSQLParser.Index_elem_optionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#index_elem}.
	 * @param ctx the parse tree
	 */
	void enterIndex_elem(PostgreSQLParser.Index_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#index_elem}.
	 * @param ctx the parse tree
	 */
	void exitIndex_elem(PostgreSQLParser.Index_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#include_}.
	 * @param ctx the parse tree
	 */
	void enterInclude_(PostgreSQLParser.Include_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#include_}.
	 * @param ctx the parse tree
	 */
	void exitInclude_(PostgreSQLParser.Include_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#index_including_params}.
	 * @param ctx the parse tree
	 */
	void enterIndex_including_params(PostgreSQLParser.Index_including_paramsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#index_including_params}.
	 * @param ctx the parse tree
	 */
	void exitIndex_including_params(PostgreSQLParser.Index_including_paramsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#collate_}.
	 * @param ctx the parse tree
	 */
	void enterCollate_(PostgreSQLParser.Collate_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#collate_}.
	 * @param ctx the parse tree
	 */
	void exitCollate_(PostgreSQLParser.Collate_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#class_}.
	 * @param ctx the parse tree
	 */
	void enterClass_(PostgreSQLParser.Class_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#class_}.
	 * @param ctx the parse tree
	 */
	void exitClass_(PostgreSQLParser.Class_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#asc_desc_}.
	 * @param ctx the parse tree
	 */
	void enterAsc_desc_(PostgreSQLParser.Asc_desc_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#asc_desc_}.
	 * @param ctx the parse tree
	 */
	void exitAsc_desc_(PostgreSQLParser.Asc_desc_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#nulls_order_}.
	 * @param ctx the parse tree
	 */
	void enterNulls_order_(PostgreSQLParser.Nulls_order_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#nulls_order_}.
	 * @param ctx the parse tree
	 */
	void exitNulls_order_(PostgreSQLParser.Nulls_order_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createfunctionstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreatefunctionstmt(PostgreSQLParser.CreatefunctionstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createfunctionstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreatefunctionstmt(PostgreSQLParser.CreatefunctionstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#or_replace_}.
	 * @param ctx the parse tree
	 */
	void enterOr_replace_(PostgreSQLParser.Or_replace_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#or_replace_}.
	 * @param ctx the parse tree
	 */
	void exitOr_replace_(PostgreSQLParser.Or_replace_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_args}.
	 * @param ctx the parse tree
	 */
	void enterFunc_args(PostgreSQLParser.Func_argsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_args}.
	 * @param ctx the parse tree
	 */
	void exitFunc_args(PostgreSQLParser.Func_argsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_args_list}.
	 * @param ctx the parse tree
	 */
	void enterFunc_args_list(PostgreSQLParser.Func_args_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_args_list}.
	 * @param ctx the parse tree
	 */
	void exitFunc_args_list(PostgreSQLParser.Func_args_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#function_with_argtypes_list}.
	 * @param ctx the parse tree
	 */
	void enterFunction_with_argtypes_list(PostgreSQLParser.Function_with_argtypes_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#function_with_argtypes_list}.
	 * @param ctx the parse tree
	 */
	void exitFunction_with_argtypes_list(PostgreSQLParser.Function_with_argtypes_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#function_with_argtypes}.
	 * @param ctx the parse tree
	 */
	void enterFunction_with_argtypes(PostgreSQLParser.Function_with_argtypesContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#function_with_argtypes}.
	 * @param ctx the parse tree
	 */
	void exitFunction_with_argtypes(PostgreSQLParser.Function_with_argtypesContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_args_with_defaults}.
	 * @param ctx the parse tree
	 */
	void enterFunc_args_with_defaults(PostgreSQLParser.Func_args_with_defaultsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_args_with_defaults}.
	 * @param ctx the parse tree
	 */
	void exitFunc_args_with_defaults(PostgreSQLParser.Func_args_with_defaultsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_args_with_defaults_list}.
	 * @param ctx the parse tree
	 */
	void enterFunc_args_with_defaults_list(PostgreSQLParser.Func_args_with_defaults_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_args_with_defaults_list}.
	 * @param ctx the parse tree
	 */
	void exitFunc_args_with_defaults_list(PostgreSQLParser.Func_args_with_defaults_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_arg}.
	 * @param ctx the parse tree
	 */
	void enterFunc_arg(PostgreSQLParser.Func_argContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_arg}.
	 * @param ctx the parse tree
	 */
	void exitFunc_arg(PostgreSQLParser.Func_argContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#arg_class}.
	 * @param ctx the parse tree
	 */
	void enterArg_class(PostgreSQLParser.Arg_classContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#arg_class}.
	 * @param ctx the parse tree
	 */
	void exitArg_class(PostgreSQLParser.Arg_classContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#param_name}.
	 * @param ctx the parse tree
	 */
	void enterParam_name(PostgreSQLParser.Param_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#param_name}.
	 * @param ctx the parse tree
	 */
	void exitParam_name(PostgreSQLParser.Param_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_return}.
	 * @param ctx the parse tree
	 */
	void enterFunc_return(PostgreSQLParser.Func_returnContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_return}.
	 * @param ctx the parse tree
	 */
	void exitFunc_return(PostgreSQLParser.Func_returnContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_type}.
	 * @param ctx the parse tree
	 */
	void enterFunc_type(PostgreSQLParser.Func_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_type}.
	 * @param ctx the parse tree
	 */
	void exitFunc_type(PostgreSQLParser.Func_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_arg_with_default}.
	 * @param ctx the parse tree
	 */
	void enterFunc_arg_with_default(PostgreSQLParser.Func_arg_with_defaultContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_arg_with_default}.
	 * @param ctx the parse tree
	 */
	void exitFunc_arg_with_default(PostgreSQLParser.Func_arg_with_defaultContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#aggr_arg}.
	 * @param ctx the parse tree
	 */
	void enterAggr_arg(PostgreSQLParser.Aggr_argContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#aggr_arg}.
	 * @param ctx the parse tree
	 */
	void exitAggr_arg(PostgreSQLParser.Aggr_argContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#aggr_args}.
	 * @param ctx the parse tree
	 */
	void enterAggr_args(PostgreSQLParser.Aggr_argsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#aggr_args}.
	 * @param ctx the parse tree
	 */
	void exitAggr_args(PostgreSQLParser.Aggr_argsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#aggr_args_list}.
	 * @param ctx the parse tree
	 */
	void enterAggr_args_list(PostgreSQLParser.Aggr_args_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#aggr_args_list}.
	 * @param ctx the parse tree
	 */
	void exitAggr_args_list(PostgreSQLParser.Aggr_args_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#aggregate_with_argtypes}.
	 * @param ctx the parse tree
	 */
	void enterAggregate_with_argtypes(PostgreSQLParser.Aggregate_with_argtypesContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#aggregate_with_argtypes}.
	 * @param ctx the parse tree
	 */
	void exitAggregate_with_argtypes(PostgreSQLParser.Aggregate_with_argtypesContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#aggregate_with_argtypes_list}.
	 * @param ctx the parse tree
	 */
	void enterAggregate_with_argtypes_list(PostgreSQLParser.Aggregate_with_argtypes_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#aggregate_with_argtypes_list}.
	 * @param ctx the parse tree
	 */
	void exitAggregate_with_argtypes_list(PostgreSQLParser.Aggregate_with_argtypes_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createfunc_opt_list}.
	 * @param ctx the parse tree
	 */
	void enterCreatefunc_opt_list(PostgreSQLParser.Createfunc_opt_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createfunc_opt_list}.
	 * @param ctx the parse tree
	 */
	void exitCreatefunc_opt_list(PostgreSQLParser.Createfunc_opt_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#common_func_opt_item}.
	 * @param ctx the parse tree
	 */
	void enterCommon_func_opt_item(PostgreSQLParser.Common_func_opt_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#common_func_opt_item}.
	 * @param ctx the parse tree
	 */
	void exitCommon_func_opt_item(PostgreSQLParser.Common_func_opt_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createfunc_opt_item}.
	 * @param ctx the parse tree
	 */
	void enterCreatefunc_opt_item(PostgreSQLParser.Createfunc_opt_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createfunc_opt_item}.
	 * @param ctx the parse tree
	 */
	void exitCreatefunc_opt_item(PostgreSQLParser.Createfunc_opt_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_as}.
	 * @param ctx the parse tree
	 */
	void enterFunc_as(PostgreSQLParser.Func_asContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_as}.
	 * @param ctx the parse tree
	 */
	void exitFunc_as(PostgreSQLParser.Func_asContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#transform_type_list}.
	 * @param ctx the parse tree
	 */
	void enterTransform_type_list(PostgreSQLParser.Transform_type_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#transform_type_list}.
	 * @param ctx the parse tree
	 */
	void exitTransform_type_list(PostgreSQLParser.Transform_type_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#definition_}.
	 * @param ctx the parse tree
	 */
	void enterDefinition_(PostgreSQLParser.Definition_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#definition_}.
	 * @param ctx the parse tree
	 */
	void exitDefinition_(PostgreSQLParser.Definition_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#table_func_column}.
	 * @param ctx the parse tree
	 */
	void enterTable_func_column(PostgreSQLParser.Table_func_columnContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#table_func_column}.
	 * @param ctx the parse tree
	 */
	void exitTable_func_column(PostgreSQLParser.Table_func_columnContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#table_func_column_list}.
	 * @param ctx the parse tree
	 */
	void enterTable_func_column_list(PostgreSQLParser.Table_func_column_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#table_func_column_list}.
	 * @param ctx the parse tree
	 */
	void exitTable_func_column_list(PostgreSQLParser.Table_func_column_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterfunctionstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterfunctionstmt(PostgreSQLParser.AlterfunctionstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterfunctionstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterfunctionstmt(PostgreSQLParser.AlterfunctionstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterfunc_opt_list}.
	 * @param ctx the parse tree
	 */
	void enterAlterfunc_opt_list(PostgreSQLParser.Alterfunc_opt_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterfunc_opt_list}.
	 * @param ctx the parse tree
	 */
	void exitAlterfunc_opt_list(PostgreSQLParser.Alterfunc_opt_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#restrict_}.
	 * @param ctx the parse tree
	 */
	void enterRestrict_(PostgreSQLParser.Restrict_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#restrict_}.
	 * @param ctx the parse tree
	 */
	void exitRestrict_(PostgreSQLParser.Restrict_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#removefuncstmt}.
	 * @param ctx the parse tree
	 */
	void enterRemovefuncstmt(PostgreSQLParser.RemovefuncstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#removefuncstmt}.
	 * @param ctx the parse tree
	 */
	void exitRemovefuncstmt(PostgreSQLParser.RemovefuncstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#removeaggrstmt}.
	 * @param ctx the parse tree
	 */
	void enterRemoveaggrstmt(PostgreSQLParser.RemoveaggrstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#removeaggrstmt}.
	 * @param ctx the parse tree
	 */
	void exitRemoveaggrstmt(PostgreSQLParser.RemoveaggrstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#removeoperstmt}.
	 * @param ctx the parse tree
	 */
	void enterRemoveoperstmt(PostgreSQLParser.RemoveoperstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#removeoperstmt}.
	 * @param ctx the parse tree
	 */
	void exitRemoveoperstmt(PostgreSQLParser.RemoveoperstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#oper_argtypes}.
	 * @param ctx the parse tree
	 */
	void enterOper_argtypes(PostgreSQLParser.Oper_argtypesContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#oper_argtypes}.
	 * @param ctx the parse tree
	 */
	void exitOper_argtypes(PostgreSQLParser.Oper_argtypesContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#any_operator}.
	 * @param ctx the parse tree
	 */
	void enterAny_operator(PostgreSQLParser.Any_operatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#any_operator}.
	 * @param ctx the parse tree
	 */
	void exitAny_operator(PostgreSQLParser.Any_operatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#operator_with_argtypes_list}.
	 * @param ctx the parse tree
	 */
	void enterOperator_with_argtypes_list(PostgreSQLParser.Operator_with_argtypes_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#operator_with_argtypes_list}.
	 * @param ctx the parse tree
	 */
	void exitOperator_with_argtypes_list(PostgreSQLParser.Operator_with_argtypes_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#operator_with_argtypes}.
	 * @param ctx the parse tree
	 */
	void enterOperator_with_argtypes(PostgreSQLParser.Operator_with_argtypesContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#operator_with_argtypes}.
	 * @param ctx the parse tree
	 */
	void exitOperator_with_argtypes(PostgreSQLParser.Operator_with_argtypesContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#dostmt}.
	 * @param ctx the parse tree
	 */
	void enterDostmt(PostgreSQLParser.DostmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#dostmt}.
	 * @param ctx the parse tree
	 */
	void exitDostmt(PostgreSQLParser.DostmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#dostmt_opt_list}.
	 * @param ctx the parse tree
	 */
	void enterDostmt_opt_list(PostgreSQLParser.Dostmt_opt_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#dostmt_opt_list}.
	 * @param ctx the parse tree
	 */
	void exitDostmt_opt_list(PostgreSQLParser.Dostmt_opt_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#dostmt_opt_item}.
	 * @param ctx the parse tree
	 */
	void enterDostmt_opt_item(PostgreSQLParser.Dostmt_opt_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#dostmt_opt_item}.
	 * @param ctx the parse tree
	 */
	void exitDostmt_opt_item(PostgreSQLParser.Dostmt_opt_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createcaststmt}.
	 * @param ctx the parse tree
	 */
	void enterCreatecaststmt(PostgreSQLParser.CreatecaststmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createcaststmt}.
	 * @param ctx the parse tree
	 */
	void exitCreatecaststmt(PostgreSQLParser.CreatecaststmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#cast_context}.
	 * @param ctx the parse tree
	 */
	void enterCast_context(PostgreSQLParser.Cast_contextContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#cast_context}.
	 * @param ctx the parse tree
	 */
	void exitCast_context(PostgreSQLParser.Cast_contextContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#dropcaststmt}.
	 * @param ctx the parse tree
	 */
	void enterDropcaststmt(PostgreSQLParser.DropcaststmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#dropcaststmt}.
	 * @param ctx the parse tree
	 */
	void exitDropcaststmt(PostgreSQLParser.DropcaststmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#if_exists_}.
	 * @param ctx the parse tree
	 */
	void enterIf_exists_(PostgreSQLParser.If_exists_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#if_exists_}.
	 * @param ctx the parse tree
	 */
	void exitIf_exists_(PostgreSQLParser.If_exists_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createtransformstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreatetransformstmt(PostgreSQLParser.CreatetransformstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createtransformstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreatetransformstmt(PostgreSQLParser.CreatetransformstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#transform_element_list}.
	 * @param ctx the parse tree
	 */
	void enterTransform_element_list(PostgreSQLParser.Transform_element_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#transform_element_list}.
	 * @param ctx the parse tree
	 */
	void exitTransform_element_list(PostgreSQLParser.Transform_element_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#droptransformstmt}.
	 * @param ctx the parse tree
	 */
	void enterDroptransformstmt(PostgreSQLParser.DroptransformstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#droptransformstmt}.
	 * @param ctx the parse tree
	 */
	void exitDroptransformstmt(PostgreSQLParser.DroptransformstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#reindexstmt}.
	 * @param ctx the parse tree
	 */
	void enterReindexstmt(PostgreSQLParser.ReindexstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#reindexstmt}.
	 * @param ctx the parse tree
	 */
	void exitReindexstmt(PostgreSQLParser.ReindexstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#reindex_target_relation}.
	 * @param ctx the parse tree
	 */
	void enterReindex_target_relation(PostgreSQLParser.Reindex_target_relationContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#reindex_target_relation}.
	 * @param ctx the parse tree
	 */
	void exitReindex_target_relation(PostgreSQLParser.Reindex_target_relationContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#reindex_target_all}.
	 * @param ctx the parse tree
	 */
	void enterReindex_target_all(PostgreSQLParser.Reindex_target_allContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#reindex_target_all}.
	 * @param ctx the parse tree
	 */
	void exitReindex_target_all(PostgreSQLParser.Reindex_target_allContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#reindex_option_list}.
	 * @param ctx the parse tree
	 */
	void enterReindex_option_list(PostgreSQLParser.Reindex_option_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#reindex_option_list}.
	 * @param ctx the parse tree
	 */
	void exitReindex_option_list(PostgreSQLParser.Reindex_option_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#altertblspcstmt}.
	 * @param ctx the parse tree
	 */
	void enterAltertblspcstmt(PostgreSQLParser.AltertblspcstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#altertblspcstmt}.
	 * @param ctx the parse tree
	 */
	void exitAltertblspcstmt(PostgreSQLParser.AltertblspcstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#renamestmt}.
	 * @param ctx the parse tree
	 */
	void enterRenamestmt(PostgreSQLParser.RenamestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#renamestmt}.
	 * @param ctx the parse tree
	 */
	void exitRenamestmt(PostgreSQLParser.RenamestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#column_}.
	 * @param ctx the parse tree
	 */
	void enterColumn_(PostgreSQLParser.Column_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#column_}.
	 * @param ctx the parse tree
	 */
	void exitColumn_(PostgreSQLParser.Column_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#set_data_}.
	 * @param ctx the parse tree
	 */
	void enterSet_data_(PostgreSQLParser.Set_data_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#set_data_}.
	 * @param ctx the parse tree
	 */
	void exitSet_data_(PostgreSQLParser.Set_data_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterobjectdependsstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterobjectdependsstmt(PostgreSQLParser.AlterobjectdependsstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterobjectdependsstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterobjectdependsstmt(PostgreSQLParser.AlterobjectdependsstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#no_}.
	 * @param ctx the parse tree
	 */
	void enterNo_(PostgreSQLParser.No_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#no_}.
	 * @param ctx the parse tree
	 */
	void exitNo_(PostgreSQLParser.No_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterobjectschemastmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterobjectschemastmt(PostgreSQLParser.AlterobjectschemastmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterobjectschemastmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterobjectschemastmt(PostgreSQLParser.AlterobjectschemastmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alteroperatorstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlteroperatorstmt(PostgreSQLParser.AlteroperatorstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alteroperatorstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlteroperatorstmt(PostgreSQLParser.AlteroperatorstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#operator_def_list}.
	 * @param ctx the parse tree
	 */
	void enterOperator_def_list(PostgreSQLParser.Operator_def_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#operator_def_list}.
	 * @param ctx the parse tree
	 */
	void exitOperator_def_list(PostgreSQLParser.Operator_def_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#operator_def_elem}.
	 * @param ctx the parse tree
	 */
	void enterOperator_def_elem(PostgreSQLParser.Operator_def_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#operator_def_elem}.
	 * @param ctx the parse tree
	 */
	void exitOperator_def_elem(PostgreSQLParser.Operator_def_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#operator_def_arg}.
	 * @param ctx the parse tree
	 */
	void enterOperator_def_arg(PostgreSQLParser.Operator_def_argContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#operator_def_arg}.
	 * @param ctx the parse tree
	 */
	void exitOperator_def_arg(PostgreSQLParser.Operator_def_argContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#altertypestmt}.
	 * @param ctx the parse tree
	 */
	void enterAltertypestmt(PostgreSQLParser.AltertypestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#altertypestmt}.
	 * @param ctx the parse tree
	 */
	void exitAltertypestmt(PostgreSQLParser.AltertypestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterownerstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterownerstmt(PostgreSQLParser.AlterownerstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterownerstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterownerstmt(PostgreSQLParser.AlterownerstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createpublicationstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreatepublicationstmt(PostgreSQLParser.CreatepublicationstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createpublicationstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreatepublicationstmt(PostgreSQLParser.CreatepublicationstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#publication_for_tables_}.
	 * @param ctx the parse tree
	 */
	void enterPublication_for_tables_(PostgreSQLParser.Publication_for_tables_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#publication_for_tables_}.
	 * @param ctx the parse tree
	 */
	void exitPublication_for_tables_(PostgreSQLParser.Publication_for_tables_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#publication_for_tables}.
	 * @param ctx the parse tree
	 */
	void enterPublication_for_tables(PostgreSQLParser.Publication_for_tablesContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#publication_for_tables}.
	 * @param ctx the parse tree
	 */
	void exitPublication_for_tables(PostgreSQLParser.Publication_for_tablesContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterpublicationstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterpublicationstmt(PostgreSQLParser.AlterpublicationstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterpublicationstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterpublicationstmt(PostgreSQLParser.AlterpublicationstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createsubscriptionstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreatesubscriptionstmt(PostgreSQLParser.CreatesubscriptionstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createsubscriptionstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreatesubscriptionstmt(PostgreSQLParser.CreatesubscriptionstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#publication_name_list}.
	 * @param ctx the parse tree
	 */
	void enterPublication_name_list(PostgreSQLParser.Publication_name_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#publication_name_list}.
	 * @param ctx the parse tree
	 */
	void exitPublication_name_list(PostgreSQLParser.Publication_name_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#publication_name_item}.
	 * @param ctx the parse tree
	 */
	void enterPublication_name_item(PostgreSQLParser.Publication_name_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#publication_name_item}.
	 * @param ctx the parse tree
	 */
	void exitPublication_name_item(PostgreSQLParser.Publication_name_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#altersubscriptionstmt}.
	 * @param ctx the parse tree
	 */
	void enterAltersubscriptionstmt(PostgreSQLParser.AltersubscriptionstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#altersubscriptionstmt}.
	 * @param ctx the parse tree
	 */
	void exitAltersubscriptionstmt(PostgreSQLParser.AltersubscriptionstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#dropsubscriptionstmt}.
	 * @param ctx the parse tree
	 */
	void enterDropsubscriptionstmt(PostgreSQLParser.DropsubscriptionstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#dropsubscriptionstmt}.
	 * @param ctx the parse tree
	 */
	void exitDropsubscriptionstmt(PostgreSQLParser.DropsubscriptionstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#rulestmt}.
	 * @param ctx the parse tree
	 */
	void enterRulestmt(PostgreSQLParser.RulestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#rulestmt}.
	 * @param ctx the parse tree
	 */
	void exitRulestmt(PostgreSQLParser.RulestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#ruleactionlist}.
	 * @param ctx the parse tree
	 */
	void enterRuleactionlist(PostgreSQLParser.RuleactionlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#ruleactionlist}.
	 * @param ctx the parse tree
	 */
	void exitRuleactionlist(PostgreSQLParser.RuleactionlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#ruleactionmulti}.
	 * @param ctx the parse tree
	 */
	void enterRuleactionmulti(PostgreSQLParser.RuleactionmultiContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#ruleactionmulti}.
	 * @param ctx the parse tree
	 */
	void exitRuleactionmulti(PostgreSQLParser.RuleactionmultiContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#ruleactionstmt}.
	 * @param ctx the parse tree
	 */
	void enterRuleactionstmt(PostgreSQLParser.RuleactionstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#ruleactionstmt}.
	 * @param ctx the parse tree
	 */
	void exitRuleactionstmt(PostgreSQLParser.RuleactionstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#ruleactionstmtOrEmpty}.
	 * @param ctx the parse tree
	 */
	void enterRuleactionstmtOrEmpty(PostgreSQLParser.RuleactionstmtOrEmptyContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#ruleactionstmtOrEmpty}.
	 * @param ctx the parse tree
	 */
	void exitRuleactionstmtOrEmpty(PostgreSQLParser.RuleactionstmtOrEmptyContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#event}.
	 * @param ctx the parse tree
	 */
	void enterEvent(PostgreSQLParser.EventContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#event}.
	 * @param ctx the parse tree
	 */
	void exitEvent(PostgreSQLParser.EventContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#instead_}.
	 * @param ctx the parse tree
	 */
	void enterInstead_(PostgreSQLParser.Instead_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#instead_}.
	 * @param ctx the parse tree
	 */
	void exitInstead_(PostgreSQLParser.Instead_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#notifystmt}.
	 * @param ctx the parse tree
	 */
	void enterNotifystmt(PostgreSQLParser.NotifystmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#notifystmt}.
	 * @param ctx the parse tree
	 */
	void exitNotifystmt(PostgreSQLParser.NotifystmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#notify_payload}.
	 * @param ctx the parse tree
	 */
	void enterNotify_payload(PostgreSQLParser.Notify_payloadContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#notify_payload}.
	 * @param ctx the parse tree
	 */
	void exitNotify_payload(PostgreSQLParser.Notify_payloadContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#listenstmt}.
	 * @param ctx the parse tree
	 */
	void enterListenstmt(PostgreSQLParser.ListenstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#listenstmt}.
	 * @param ctx the parse tree
	 */
	void exitListenstmt(PostgreSQLParser.ListenstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#unlistenstmt}.
	 * @param ctx the parse tree
	 */
	void enterUnlistenstmt(PostgreSQLParser.UnlistenstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#unlistenstmt}.
	 * @param ctx the parse tree
	 */
	void exitUnlistenstmt(PostgreSQLParser.UnlistenstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#transactionstmt}.
	 * @param ctx the parse tree
	 */
	void enterTransactionstmt(PostgreSQLParser.TransactionstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#transactionstmt}.
	 * @param ctx the parse tree
	 */
	void exitTransactionstmt(PostgreSQLParser.TransactionstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#transaction_}.
	 * @param ctx the parse tree
	 */
	void enterTransaction_(PostgreSQLParser.Transaction_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#transaction_}.
	 * @param ctx the parse tree
	 */
	void exitTransaction_(PostgreSQLParser.Transaction_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#transaction_mode_item}.
	 * @param ctx the parse tree
	 */
	void enterTransaction_mode_item(PostgreSQLParser.Transaction_mode_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#transaction_mode_item}.
	 * @param ctx the parse tree
	 */
	void exitTransaction_mode_item(PostgreSQLParser.Transaction_mode_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#transaction_mode_list}.
	 * @param ctx the parse tree
	 */
	void enterTransaction_mode_list(PostgreSQLParser.Transaction_mode_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#transaction_mode_list}.
	 * @param ctx the parse tree
	 */
	void exitTransaction_mode_list(PostgreSQLParser.Transaction_mode_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#transaction_mode_list_or_empty}.
	 * @param ctx the parse tree
	 */
	void enterTransaction_mode_list_or_empty(PostgreSQLParser.Transaction_mode_list_or_emptyContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#transaction_mode_list_or_empty}.
	 * @param ctx the parse tree
	 */
	void exitTransaction_mode_list_or_empty(PostgreSQLParser.Transaction_mode_list_or_emptyContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#transaction_chain_}.
	 * @param ctx the parse tree
	 */
	void enterTransaction_chain_(PostgreSQLParser.Transaction_chain_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#transaction_chain_}.
	 * @param ctx the parse tree
	 */
	void exitTransaction_chain_(PostgreSQLParser.Transaction_chain_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#viewstmt}.
	 * @param ctx the parse tree
	 */
	void enterViewstmt(PostgreSQLParser.ViewstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#viewstmt}.
	 * @param ctx the parse tree
	 */
	void exitViewstmt(PostgreSQLParser.ViewstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#check_option_}.
	 * @param ctx the parse tree
	 */
	void enterCheck_option_(PostgreSQLParser.Check_option_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#check_option_}.
	 * @param ctx the parse tree
	 */
	void exitCheck_option_(PostgreSQLParser.Check_option_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#loadstmt}.
	 * @param ctx the parse tree
	 */
	void enterLoadstmt(PostgreSQLParser.LoadstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#loadstmt}.
	 * @param ctx the parse tree
	 */
	void exitLoadstmt(PostgreSQLParser.LoadstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createdbstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreatedbstmt(PostgreSQLParser.CreatedbstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createdbstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreatedbstmt(PostgreSQLParser.CreatedbstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createdb_opt_list}.
	 * @param ctx the parse tree
	 */
	void enterCreatedb_opt_list(PostgreSQLParser.Createdb_opt_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createdb_opt_list}.
	 * @param ctx the parse tree
	 */
	void exitCreatedb_opt_list(PostgreSQLParser.Createdb_opt_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createdb_opt_items}.
	 * @param ctx the parse tree
	 */
	void enterCreatedb_opt_items(PostgreSQLParser.Createdb_opt_itemsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createdb_opt_items}.
	 * @param ctx the parse tree
	 */
	void exitCreatedb_opt_items(PostgreSQLParser.Createdb_opt_itemsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createdb_opt_item}.
	 * @param ctx the parse tree
	 */
	void enterCreatedb_opt_item(PostgreSQLParser.Createdb_opt_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createdb_opt_item}.
	 * @param ctx the parse tree
	 */
	void exitCreatedb_opt_item(PostgreSQLParser.Createdb_opt_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createdb_opt_name}.
	 * @param ctx the parse tree
	 */
	void enterCreatedb_opt_name(PostgreSQLParser.Createdb_opt_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createdb_opt_name}.
	 * @param ctx the parse tree
	 */
	void exitCreatedb_opt_name(PostgreSQLParser.Createdb_opt_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#equal_}.
	 * @param ctx the parse tree
	 */
	void enterEqual_(PostgreSQLParser.Equal_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#equal_}.
	 * @param ctx the parse tree
	 */
	void exitEqual_(PostgreSQLParser.Equal_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterdatabasestmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterdatabasestmt(PostgreSQLParser.AlterdatabasestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterdatabasestmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterdatabasestmt(PostgreSQLParser.AlterdatabasestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterdatabasesetstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterdatabasesetstmt(PostgreSQLParser.AlterdatabasesetstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterdatabasesetstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterdatabasesetstmt(PostgreSQLParser.AlterdatabasesetstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#dropdbstmt}.
	 * @param ctx the parse tree
	 */
	void enterDropdbstmt(PostgreSQLParser.DropdbstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#dropdbstmt}.
	 * @param ctx the parse tree
	 */
	void exitDropdbstmt(PostgreSQLParser.DropdbstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#drop_option_list}.
	 * @param ctx the parse tree
	 */
	void enterDrop_option_list(PostgreSQLParser.Drop_option_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#drop_option_list}.
	 * @param ctx the parse tree
	 */
	void exitDrop_option_list(PostgreSQLParser.Drop_option_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#drop_option}.
	 * @param ctx the parse tree
	 */
	void enterDrop_option(PostgreSQLParser.Drop_optionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#drop_option}.
	 * @param ctx the parse tree
	 */
	void exitDrop_option(PostgreSQLParser.Drop_optionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#altercollationstmt}.
	 * @param ctx the parse tree
	 */
	void enterAltercollationstmt(PostgreSQLParser.AltercollationstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#altercollationstmt}.
	 * @param ctx the parse tree
	 */
	void exitAltercollationstmt(PostgreSQLParser.AltercollationstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#altersystemstmt}.
	 * @param ctx the parse tree
	 */
	void enterAltersystemstmt(PostgreSQLParser.AltersystemstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#altersystemstmt}.
	 * @param ctx the parse tree
	 */
	void exitAltersystemstmt(PostgreSQLParser.AltersystemstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createdomainstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreatedomainstmt(PostgreSQLParser.CreatedomainstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createdomainstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreatedomainstmt(PostgreSQLParser.CreatedomainstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alterdomainstmt}.
	 * @param ctx the parse tree
	 */
	void enterAlterdomainstmt(PostgreSQLParser.AlterdomainstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alterdomainstmt}.
	 * @param ctx the parse tree
	 */
	void exitAlterdomainstmt(PostgreSQLParser.AlterdomainstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#as_}.
	 * @param ctx the parse tree
	 */
	void enterAs_(PostgreSQLParser.As_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#as_}.
	 * @param ctx the parse tree
	 */
	void exitAs_(PostgreSQLParser.As_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#altertsdictionarystmt}.
	 * @param ctx the parse tree
	 */
	void enterAltertsdictionarystmt(PostgreSQLParser.AltertsdictionarystmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#altertsdictionarystmt}.
	 * @param ctx the parse tree
	 */
	void exitAltertsdictionarystmt(PostgreSQLParser.AltertsdictionarystmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#altertsconfigurationstmt}.
	 * @param ctx the parse tree
	 */
	void enterAltertsconfigurationstmt(PostgreSQLParser.AltertsconfigurationstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#altertsconfigurationstmt}.
	 * @param ctx the parse tree
	 */
	void exitAltertsconfigurationstmt(PostgreSQLParser.AltertsconfigurationstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#any_with}.
	 * @param ctx the parse tree
	 */
	void enterAny_with(PostgreSQLParser.Any_withContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#any_with}.
	 * @param ctx the parse tree
	 */
	void exitAny_with(PostgreSQLParser.Any_withContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#createconversionstmt}.
	 * @param ctx the parse tree
	 */
	void enterCreateconversionstmt(PostgreSQLParser.CreateconversionstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#createconversionstmt}.
	 * @param ctx the parse tree
	 */
	void exitCreateconversionstmt(PostgreSQLParser.CreateconversionstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#clusterstmt}.
	 * @param ctx the parse tree
	 */
	void enterClusterstmt(PostgreSQLParser.ClusterstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#clusterstmt}.
	 * @param ctx the parse tree
	 */
	void exitClusterstmt(PostgreSQLParser.ClusterstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#cluster_index_specification}.
	 * @param ctx the parse tree
	 */
	void enterCluster_index_specification(PostgreSQLParser.Cluster_index_specificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#cluster_index_specification}.
	 * @param ctx the parse tree
	 */
	void exitCluster_index_specification(PostgreSQLParser.Cluster_index_specificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#vacuumstmt}.
	 * @param ctx the parse tree
	 */
	void enterVacuumstmt(PostgreSQLParser.VacuumstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#vacuumstmt}.
	 * @param ctx the parse tree
	 */
	void exitVacuumstmt(PostgreSQLParser.VacuumstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#analyzestmt}.
	 * @param ctx the parse tree
	 */
	void enterAnalyzestmt(PostgreSQLParser.AnalyzestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#analyzestmt}.
	 * @param ctx the parse tree
	 */
	void exitAnalyzestmt(PostgreSQLParser.AnalyzestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#utility_option_list}.
	 * @param ctx the parse tree
	 */
	void enterUtility_option_list(PostgreSQLParser.Utility_option_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#utility_option_list}.
	 * @param ctx the parse tree
	 */
	void exitUtility_option_list(PostgreSQLParser.Utility_option_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#vac_analyze_option_list}.
	 * @param ctx the parse tree
	 */
	void enterVac_analyze_option_list(PostgreSQLParser.Vac_analyze_option_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#vac_analyze_option_list}.
	 * @param ctx the parse tree
	 */
	void exitVac_analyze_option_list(PostgreSQLParser.Vac_analyze_option_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#analyze_keyword}.
	 * @param ctx the parse tree
	 */
	void enterAnalyze_keyword(PostgreSQLParser.Analyze_keywordContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#analyze_keyword}.
	 * @param ctx the parse tree
	 */
	void exitAnalyze_keyword(PostgreSQLParser.Analyze_keywordContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#utility_option_elem}.
	 * @param ctx the parse tree
	 */
	void enterUtility_option_elem(PostgreSQLParser.Utility_option_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#utility_option_elem}.
	 * @param ctx the parse tree
	 */
	void exitUtility_option_elem(PostgreSQLParser.Utility_option_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#utility_option_name}.
	 * @param ctx the parse tree
	 */
	void enterUtility_option_name(PostgreSQLParser.Utility_option_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#utility_option_name}.
	 * @param ctx the parse tree
	 */
	void exitUtility_option_name(PostgreSQLParser.Utility_option_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#utility_option_arg}.
	 * @param ctx the parse tree
	 */
	void enterUtility_option_arg(PostgreSQLParser.Utility_option_argContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#utility_option_arg}.
	 * @param ctx the parse tree
	 */
	void exitUtility_option_arg(PostgreSQLParser.Utility_option_argContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#vac_analyze_option_elem}.
	 * @param ctx the parse tree
	 */
	void enterVac_analyze_option_elem(PostgreSQLParser.Vac_analyze_option_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#vac_analyze_option_elem}.
	 * @param ctx the parse tree
	 */
	void exitVac_analyze_option_elem(PostgreSQLParser.Vac_analyze_option_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#vac_analyze_option_name}.
	 * @param ctx the parse tree
	 */
	void enterVac_analyze_option_name(PostgreSQLParser.Vac_analyze_option_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#vac_analyze_option_name}.
	 * @param ctx the parse tree
	 */
	void exitVac_analyze_option_name(PostgreSQLParser.Vac_analyze_option_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#vac_analyze_option_arg}.
	 * @param ctx the parse tree
	 */
	void enterVac_analyze_option_arg(PostgreSQLParser.Vac_analyze_option_argContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#vac_analyze_option_arg}.
	 * @param ctx the parse tree
	 */
	void exitVac_analyze_option_arg(PostgreSQLParser.Vac_analyze_option_argContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#analyze_}.
	 * @param ctx the parse tree
	 */
	void enterAnalyze_(PostgreSQLParser.Analyze_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#analyze_}.
	 * @param ctx the parse tree
	 */
	void exitAnalyze_(PostgreSQLParser.Analyze_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#verbose_}.
	 * @param ctx the parse tree
	 */
	void enterVerbose_(PostgreSQLParser.Verbose_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#verbose_}.
	 * @param ctx the parse tree
	 */
	void exitVerbose_(PostgreSQLParser.Verbose_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#full_}.
	 * @param ctx the parse tree
	 */
	void enterFull_(PostgreSQLParser.Full_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#full_}.
	 * @param ctx the parse tree
	 */
	void exitFull_(PostgreSQLParser.Full_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#freeze_}.
	 * @param ctx the parse tree
	 */
	void enterFreeze_(PostgreSQLParser.Freeze_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#freeze_}.
	 * @param ctx the parse tree
	 */
	void exitFreeze_(PostgreSQLParser.Freeze_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#name_list_}.
	 * @param ctx the parse tree
	 */
	void enterName_list_(PostgreSQLParser.Name_list_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#name_list_}.
	 * @param ctx the parse tree
	 */
	void exitName_list_(PostgreSQLParser.Name_list_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#vacuum_relation}.
	 * @param ctx the parse tree
	 */
	void enterVacuum_relation(PostgreSQLParser.Vacuum_relationContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#vacuum_relation}.
	 * @param ctx the parse tree
	 */
	void exitVacuum_relation(PostgreSQLParser.Vacuum_relationContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#vacuum_relation_list}.
	 * @param ctx the parse tree
	 */
	void enterVacuum_relation_list(PostgreSQLParser.Vacuum_relation_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#vacuum_relation_list}.
	 * @param ctx the parse tree
	 */
	void exitVacuum_relation_list(PostgreSQLParser.Vacuum_relation_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#vacuum_relation_list_}.
	 * @param ctx the parse tree
	 */
	void enterVacuum_relation_list_(PostgreSQLParser.Vacuum_relation_list_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#vacuum_relation_list_}.
	 * @param ctx the parse tree
	 */
	void exitVacuum_relation_list_(PostgreSQLParser.Vacuum_relation_list_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#explainstmt}.
	 * @param ctx the parse tree
	 */
	void enterExplainstmt(PostgreSQLParser.ExplainstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#explainstmt}.
	 * @param ctx the parse tree
	 */
	void exitExplainstmt(PostgreSQLParser.ExplainstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#explainablestmt}.
	 * @param ctx the parse tree
	 */
	void enterExplainablestmt(PostgreSQLParser.ExplainablestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#explainablestmt}.
	 * @param ctx the parse tree
	 */
	void exitExplainablestmt(PostgreSQLParser.ExplainablestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#explain_option_list}.
	 * @param ctx the parse tree
	 */
	void enterExplain_option_list(PostgreSQLParser.Explain_option_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#explain_option_list}.
	 * @param ctx the parse tree
	 */
	void exitExplain_option_list(PostgreSQLParser.Explain_option_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#explain_option_elem}.
	 * @param ctx the parse tree
	 */
	void enterExplain_option_elem(PostgreSQLParser.Explain_option_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#explain_option_elem}.
	 * @param ctx the parse tree
	 */
	void exitExplain_option_elem(PostgreSQLParser.Explain_option_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#explain_option_name}.
	 * @param ctx the parse tree
	 */
	void enterExplain_option_name(PostgreSQLParser.Explain_option_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#explain_option_name}.
	 * @param ctx the parse tree
	 */
	void exitExplain_option_name(PostgreSQLParser.Explain_option_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#explain_option_arg}.
	 * @param ctx the parse tree
	 */
	void enterExplain_option_arg(PostgreSQLParser.Explain_option_argContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#explain_option_arg}.
	 * @param ctx the parse tree
	 */
	void exitExplain_option_arg(PostgreSQLParser.Explain_option_argContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#preparestmt}.
	 * @param ctx the parse tree
	 */
	void enterPreparestmt(PostgreSQLParser.PreparestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#preparestmt}.
	 * @param ctx the parse tree
	 */
	void exitPreparestmt(PostgreSQLParser.PreparestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#prep_type_clause}.
	 * @param ctx the parse tree
	 */
	void enterPrep_type_clause(PostgreSQLParser.Prep_type_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#prep_type_clause}.
	 * @param ctx the parse tree
	 */
	void exitPrep_type_clause(PostgreSQLParser.Prep_type_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#preparablestmt}.
	 * @param ctx the parse tree
	 */
	void enterPreparablestmt(PostgreSQLParser.PreparablestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#preparablestmt}.
	 * @param ctx the parse tree
	 */
	void exitPreparablestmt(PostgreSQLParser.PreparablestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#executestmt}.
	 * @param ctx the parse tree
	 */
	void enterExecutestmt(PostgreSQLParser.ExecutestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#executestmt}.
	 * @param ctx the parse tree
	 */
	void exitExecutestmt(PostgreSQLParser.ExecutestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#execute_param_clause}.
	 * @param ctx the parse tree
	 */
	void enterExecute_param_clause(PostgreSQLParser.Execute_param_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#execute_param_clause}.
	 * @param ctx the parse tree
	 */
	void exitExecute_param_clause(PostgreSQLParser.Execute_param_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#deallocatestmt}.
	 * @param ctx the parse tree
	 */
	void enterDeallocatestmt(PostgreSQLParser.DeallocatestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#deallocatestmt}.
	 * @param ctx the parse tree
	 */
	void exitDeallocatestmt(PostgreSQLParser.DeallocatestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#insertstmt}.
	 * @param ctx the parse tree
	 */
	void enterInsertstmt(PostgreSQLParser.InsertstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#insertstmt}.
	 * @param ctx the parse tree
	 */
	void exitInsertstmt(PostgreSQLParser.InsertstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#insert_target}.
	 * @param ctx the parse tree
	 */
	void enterInsert_target(PostgreSQLParser.Insert_targetContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#insert_target}.
	 * @param ctx the parse tree
	 */
	void exitInsert_target(PostgreSQLParser.Insert_targetContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#insert_rest}.
	 * @param ctx the parse tree
	 */
	void enterInsert_rest(PostgreSQLParser.Insert_restContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#insert_rest}.
	 * @param ctx the parse tree
	 */
	void exitInsert_rest(PostgreSQLParser.Insert_restContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#override_kind}.
	 * @param ctx the parse tree
	 */
	void enterOverride_kind(PostgreSQLParser.Override_kindContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#override_kind}.
	 * @param ctx the parse tree
	 */
	void exitOverride_kind(PostgreSQLParser.Override_kindContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#insert_column_list}.
	 * @param ctx the parse tree
	 */
	void enterInsert_column_list(PostgreSQLParser.Insert_column_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#insert_column_list}.
	 * @param ctx the parse tree
	 */
	void exitInsert_column_list(PostgreSQLParser.Insert_column_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#insert_column_item}.
	 * @param ctx the parse tree
	 */
	void enterInsert_column_item(PostgreSQLParser.Insert_column_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#insert_column_item}.
	 * @param ctx the parse tree
	 */
	void exitInsert_column_item(PostgreSQLParser.Insert_column_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#on_conflict_}.
	 * @param ctx the parse tree
	 */
	void enterOn_conflict_(PostgreSQLParser.On_conflict_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#on_conflict_}.
	 * @param ctx the parse tree
	 */
	void exitOn_conflict_(PostgreSQLParser.On_conflict_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#conf_expr_}.
	 * @param ctx the parse tree
	 */
	void enterConf_expr_(PostgreSQLParser.Conf_expr_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#conf_expr_}.
	 * @param ctx the parse tree
	 */
	void exitConf_expr_(PostgreSQLParser.Conf_expr_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#returning_clause}.
	 * @param ctx the parse tree
	 */
	void enterReturning_clause(PostgreSQLParser.Returning_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#returning_clause}.
	 * @param ctx the parse tree
	 */
	void exitReturning_clause(PostgreSQLParser.Returning_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#mergestmt}.
	 * @param ctx the parse tree
	 */
	void enterMergestmt(PostgreSQLParser.MergestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#mergestmt}.
	 * @param ctx the parse tree
	 */
	void exitMergestmt(PostgreSQLParser.MergestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#merge_insert_clause}.
	 * @param ctx the parse tree
	 */
	void enterMerge_insert_clause(PostgreSQLParser.Merge_insert_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#merge_insert_clause}.
	 * @param ctx the parse tree
	 */
	void exitMerge_insert_clause(PostgreSQLParser.Merge_insert_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#merge_update_clause}.
	 * @param ctx the parse tree
	 */
	void enterMerge_update_clause(PostgreSQLParser.Merge_update_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#merge_update_clause}.
	 * @param ctx the parse tree
	 */
	void exitMerge_update_clause(PostgreSQLParser.Merge_update_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#merge_delete_clause}.
	 * @param ctx the parse tree
	 */
	void enterMerge_delete_clause(PostgreSQLParser.Merge_delete_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#merge_delete_clause}.
	 * @param ctx the parse tree
	 */
	void exitMerge_delete_clause(PostgreSQLParser.Merge_delete_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#deletestmt}.
	 * @param ctx the parse tree
	 */
	void enterDeletestmt(PostgreSQLParser.DeletestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#deletestmt}.
	 * @param ctx the parse tree
	 */
	void exitDeletestmt(PostgreSQLParser.DeletestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#using_clause}.
	 * @param ctx the parse tree
	 */
	void enterUsing_clause(PostgreSQLParser.Using_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#using_clause}.
	 * @param ctx the parse tree
	 */
	void exitUsing_clause(PostgreSQLParser.Using_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#lockstmt}.
	 * @param ctx the parse tree
	 */
	void enterLockstmt(PostgreSQLParser.LockstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#lockstmt}.
	 * @param ctx the parse tree
	 */
	void exitLockstmt(PostgreSQLParser.LockstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#lock_}.
	 * @param ctx the parse tree
	 */
	void enterLock_(PostgreSQLParser.Lock_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#lock_}.
	 * @param ctx the parse tree
	 */
	void exitLock_(PostgreSQLParser.Lock_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#lock_type}.
	 * @param ctx the parse tree
	 */
	void enterLock_type(PostgreSQLParser.Lock_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#lock_type}.
	 * @param ctx the parse tree
	 */
	void exitLock_type(PostgreSQLParser.Lock_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#nowait_}.
	 * @param ctx the parse tree
	 */
	void enterNowait_(PostgreSQLParser.Nowait_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#nowait_}.
	 * @param ctx the parse tree
	 */
	void exitNowait_(PostgreSQLParser.Nowait_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#nowait_or_skip_}.
	 * @param ctx the parse tree
	 */
	void enterNowait_or_skip_(PostgreSQLParser.Nowait_or_skip_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#nowait_or_skip_}.
	 * @param ctx the parse tree
	 */
	void exitNowait_or_skip_(PostgreSQLParser.Nowait_or_skip_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#updatestmt}.
	 * @param ctx the parse tree
	 */
	void enterUpdatestmt(PostgreSQLParser.UpdatestmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#updatestmt}.
	 * @param ctx the parse tree
	 */
	void exitUpdatestmt(PostgreSQLParser.UpdatestmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#set_clause_list}.
	 * @param ctx the parse tree
	 */
	void enterSet_clause_list(PostgreSQLParser.Set_clause_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#set_clause_list}.
	 * @param ctx the parse tree
	 */
	void exitSet_clause_list(PostgreSQLParser.Set_clause_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#set_clause}.
	 * @param ctx the parse tree
	 */
	void enterSet_clause(PostgreSQLParser.Set_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#set_clause}.
	 * @param ctx the parse tree
	 */
	void exitSet_clause(PostgreSQLParser.Set_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#set_target}.
	 * @param ctx the parse tree
	 */
	void enterSet_target(PostgreSQLParser.Set_targetContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#set_target}.
	 * @param ctx the parse tree
	 */
	void exitSet_target(PostgreSQLParser.Set_targetContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#set_target_list}.
	 * @param ctx the parse tree
	 */
	void enterSet_target_list(PostgreSQLParser.Set_target_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#set_target_list}.
	 * @param ctx the parse tree
	 */
	void exitSet_target_list(PostgreSQLParser.Set_target_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#declarecursorstmt}.
	 * @param ctx the parse tree
	 */
	void enterDeclarecursorstmt(PostgreSQLParser.DeclarecursorstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#declarecursorstmt}.
	 * @param ctx the parse tree
	 */
	void exitDeclarecursorstmt(PostgreSQLParser.DeclarecursorstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#cursor_name}.
	 * @param ctx the parse tree
	 */
	void enterCursor_name(PostgreSQLParser.Cursor_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#cursor_name}.
	 * @param ctx the parse tree
	 */
	void exitCursor_name(PostgreSQLParser.Cursor_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#cursor_options}.
	 * @param ctx the parse tree
	 */
	void enterCursor_options(PostgreSQLParser.Cursor_optionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#cursor_options}.
	 * @param ctx the parse tree
	 */
	void exitCursor_options(PostgreSQLParser.Cursor_optionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#hold_}.
	 * @param ctx the parse tree
	 */
	void enterHold_(PostgreSQLParser.Hold_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#hold_}.
	 * @param ctx the parse tree
	 */
	void exitHold_(PostgreSQLParser.Hold_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#selectstmt}.
	 * @param ctx the parse tree
	 */
	void enterSelectstmt(PostgreSQLParser.SelectstmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#selectstmt}.
	 * @param ctx the parse tree
	 */
	void exitSelectstmt(PostgreSQLParser.SelectstmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#select_with_parens}.
	 * @param ctx the parse tree
	 */
	void enterSelect_with_parens(PostgreSQLParser.Select_with_parensContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#select_with_parens}.
	 * @param ctx the parse tree
	 */
	void exitSelect_with_parens(PostgreSQLParser.Select_with_parensContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#select_no_parens}.
	 * @param ctx the parse tree
	 */
	void enterSelect_no_parens(PostgreSQLParser.Select_no_parensContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#select_no_parens}.
	 * @param ctx the parse tree
	 */
	void exitSelect_no_parens(PostgreSQLParser.Select_no_parensContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#select_clause}.
	 * @param ctx the parse tree
	 */
	void enterSelect_clause(PostgreSQLParser.Select_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#select_clause}.
	 * @param ctx the parse tree
	 */
	void exitSelect_clause(PostgreSQLParser.Select_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#simple_select_intersect}.
	 * @param ctx the parse tree
	 */
	void enterSimple_select_intersect(PostgreSQLParser.Simple_select_intersectContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#simple_select_intersect}.
	 * @param ctx the parse tree
	 */
	void exitSimple_select_intersect(PostgreSQLParser.Simple_select_intersectContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#simple_select_pramary}.
	 * @param ctx the parse tree
	 */
	void enterSimple_select_pramary(PostgreSQLParser.Simple_select_pramaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#simple_select_pramary}.
	 * @param ctx the parse tree
	 */
	void exitSimple_select_pramary(PostgreSQLParser.Simple_select_pramaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#with_clause}.
	 * @param ctx the parse tree
	 */
	void enterWith_clause(PostgreSQLParser.With_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#with_clause}.
	 * @param ctx the parse tree
	 */
	void exitWith_clause(PostgreSQLParser.With_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#cte_list}.
	 * @param ctx the parse tree
	 */
	void enterCte_list(PostgreSQLParser.Cte_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#cte_list}.
	 * @param ctx the parse tree
	 */
	void exitCte_list(PostgreSQLParser.Cte_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#common_table_expr}.
	 * @param ctx the parse tree
	 */
	void enterCommon_table_expr(PostgreSQLParser.Common_table_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#common_table_expr}.
	 * @param ctx the parse tree
	 */
	void exitCommon_table_expr(PostgreSQLParser.Common_table_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#materialized_}.
	 * @param ctx the parse tree
	 */
	void enterMaterialized_(PostgreSQLParser.Materialized_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#materialized_}.
	 * @param ctx the parse tree
	 */
	void exitMaterialized_(PostgreSQLParser.Materialized_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#with_clause_}.
	 * @param ctx the parse tree
	 */
	void enterWith_clause_(PostgreSQLParser.With_clause_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#with_clause_}.
	 * @param ctx the parse tree
	 */
	void exitWith_clause_(PostgreSQLParser.With_clause_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#into_clause}.
	 * @param ctx the parse tree
	 */
	void enterInto_clause(PostgreSQLParser.Into_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#into_clause}.
	 * @param ctx the parse tree
	 */
	void exitInto_clause(PostgreSQLParser.Into_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#strict_}.
	 * @param ctx the parse tree
	 */
	void enterStrict_(PostgreSQLParser.Strict_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#strict_}.
	 * @param ctx the parse tree
	 */
	void exitStrict_(PostgreSQLParser.Strict_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#opttempTableName}.
	 * @param ctx the parse tree
	 */
	void enterOpttempTableName(PostgreSQLParser.OpttempTableNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#opttempTableName}.
	 * @param ctx the parse tree
	 */
	void exitOpttempTableName(PostgreSQLParser.OpttempTableNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#table_}.
	 * @param ctx the parse tree
	 */
	void enterTable_(PostgreSQLParser.Table_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#table_}.
	 * @param ctx the parse tree
	 */
	void exitTable_(PostgreSQLParser.Table_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#all_or_distinct}.
	 * @param ctx the parse tree
	 */
	void enterAll_or_distinct(PostgreSQLParser.All_or_distinctContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#all_or_distinct}.
	 * @param ctx the parse tree
	 */
	void exitAll_or_distinct(PostgreSQLParser.All_or_distinctContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#distinct_clause}.
	 * @param ctx the parse tree
	 */
	void enterDistinct_clause(PostgreSQLParser.Distinct_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#distinct_clause}.
	 * @param ctx the parse tree
	 */
	void exitDistinct_clause(PostgreSQLParser.Distinct_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#all_clause_}.
	 * @param ctx the parse tree
	 */
	void enterAll_clause_(PostgreSQLParser.All_clause_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#all_clause_}.
	 * @param ctx the parse tree
	 */
	void exitAll_clause_(PostgreSQLParser.All_clause_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#sort_clause_}.
	 * @param ctx the parse tree
	 */
	void enterSort_clause_(PostgreSQLParser.Sort_clause_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#sort_clause_}.
	 * @param ctx the parse tree
	 */
	void exitSort_clause_(PostgreSQLParser.Sort_clause_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#sort_clause}.
	 * @param ctx the parse tree
	 */
	void enterSort_clause(PostgreSQLParser.Sort_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#sort_clause}.
	 * @param ctx the parse tree
	 */
	void exitSort_clause(PostgreSQLParser.Sort_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#sortby_list}.
	 * @param ctx the parse tree
	 */
	void enterSortby_list(PostgreSQLParser.Sortby_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#sortby_list}.
	 * @param ctx the parse tree
	 */
	void exitSortby_list(PostgreSQLParser.Sortby_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#sortby}.
	 * @param ctx the parse tree
	 */
	void enterSortby(PostgreSQLParser.SortbyContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#sortby}.
	 * @param ctx the parse tree
	 */
	void exitSortby(PostgreSQLParser.SortbyContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#select_limit}.
	 * @param ctx the parse tree
	 */
	void enterSelect_limit(PostgreSQLParser.Select_limitContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#select_limit}.
	 * @param ctx the parse tree
	 */
	void exitSelect_limit(PostgreSQLParser.Select_limitContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#select_limit_}.
	 * @param ctx the parse tree
	 */
	void enterSelect_limit_(PostgreSQLParser.Select_limit_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#select_limit_}.
	 * @param ctx the parse tree
	 */
	void exitSelect_limit_(PostgreSQLParser.Select_limit_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#limit_clause}.
	 * @param ctx the parse tree
	 */
	void enterLimit_clause(PostgreSQLParser.Limit_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#limit_clause}.
	 * @param ctx the parse tree
	 */
	void exitLimit_clause(PostgreSQLParser.Limit_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#offset_clause}.
	 * @param ctx the parse tree
	 */
	void enterOffset_clause(PostgreSQLParser.Offset_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#offset_clause}.
	 * @param ctx the parse tree
	 */
	void exitOffset_clause(PostgreSQLParser.Offset_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#select_limit_value}.
	 * @param ctx the parse tree
	 */
	void enterSelect_limit_value(PostgreSQLParser.Select_limit_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#select_limit_value}.
	 * @param ctx the parse tree
	 */
	void exitSelect_limit_value(PostgreSQLParser.Select_limit_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#select_offset_value}.
	 * @param ctx the parse tree
	 */
	void enterSelect_offset_value(PostgreSQLParser.Select_offset_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#select_offset_value}.
	 * @param ctx the parse tree
	 */
	void exitSelect_offset_value(PostgreSQLParser.Select_offset_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#select_fetch_first_value}.
	 * @param ctx the parse tree
	 */
	void enterSelect_fetch_first_value(PostgreSQLParser.Select_fetch_first_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#select_fetch_first_value}.
	 * @param ctx the parse tree
	 */
	void exitSelect_fetch_first_value(PostgreSQLParser.Select_fetch_first_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#i_or_f_const}.
	 * @param ctx the parse tree
	 */
	void enterI_or_f_const(PostgreSQLParser.I_or_f_constContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#i_or_f_const}.
	 * @param ctx the parse tree
	 */
	void exitI_or_f_const(PostgreSQLParser.I_or_f_constContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#row_or_rows}.
	 * @param ctx the parse tree
	 */
	void enterRow_or_rows(PostgreSQLParser.Row_or_rowsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#row_or_rows}.
	 * @param ctx the parse tree
	 */
	void exitRow_or_rows(PostgreSQLParser.Row_or_rowsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#first_or_next}.
	 * @param ctx the parse tree
	 */
	void enterFirst_or_next(PostgreSQLParser.First_or_nextContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#first_or_next}.
	 * @param ctx the parse tree
	 */
	void exitFirst_or_next(PostgreSQLParser.First_or_nextContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#group_clause}.
	 * @param ctx the parse tree
	 */
	void enterGroup_clause(PostgreSQLParser.Group_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#group_clause}.
	 * @param ctx the parse tree
	 */
	void exitGroup_clause(PostgreSQLParser.Group_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#group_by_list}.
	 * @param ctx the parse tree
	 */
	void enterGroup_by_list(PostgreSQLParser.Group_by_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#group_by_list}.
	 * @param ctx the parse tree
	 */
	void exitGroup_by_list(PostgreSQLParser.Group_by_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#group_by_item}.
	 * @param ctx the parse tree
	 */
	void enterGroup_by_item(PostgreSQLParser.Group_by_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#group_by_item}.
	 * @param ctx the parse tree
	 */
	void exitGroup_by_item(PostgreSQLParser.Group_by_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#empty_grouping_set}.
	 * @param ctx the parse tree
	 */
	void enterEmpty_grouping_set(PostgreSQLParser.Empty_grouping_setContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#empty_grouping_set}.
	 * @param ctx the parse tree
	 */
	void exitEmpty_grouping_set(PostgreSQLParser.Empty_grouping_setContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#rollup_clause}.
	 * @param ctx the parse tree
	 */
	void enterRollup_clause(PostgreSQLParser.Rollup_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#rollup_clause}.
	 * @param ctx the parse tree
	 */
	void exitRollup_clause(PostgreSQLParser.Rollup_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#cube_clause}.
	 * @param ctx the parse tree
	 */
	void enterCube_clause(PostgreSQLParser.Cube_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#cube_clause}.
	 * @param ctx the parse tree
	 */
	void exitCube_clause(PostgreSQLParser.Cube_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#grouping_sets_clause}.
	 * @param ctx the parse tree
	 */
	void enterGrouping_sets_clause(PostgreSQLParser.Grouping_sets_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#grouping_sets_clause}.
	 * @param ctx the parse tree
	 */
	void exitGrouping_sets_clause(PostgreSQLParser.Grouping_sets_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#having_clause}.
	 * @param ctx the parse tree
	 */
	void enterHaving_clause(PostgreSQLParser.Having_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#having_clause}.
	 * @param ctx the parse tree
	 */
	void exitHaving_clause(PostgreSQLParser.Having_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#for_locking_clause}.
	 * @param ctx the parse tree
	 */
	void enterFor_locking_clause(PostgreSQLParser.For_locking_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#for_locking_clause}.
	 * @param ctx the parse tree
	 */
	void exitFor_locking_clause(PostgreSQLParser.For_locking_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#for_locking_clause_}.
	 * @param ctx the parse tree
	 */
	void enterFor_locking_clause_(PostgreSQLParser.For_locking_clause_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#for_locking_clause_}.
	 * @param ctx the parse tree
	 */
	void exitFor_locking_clause_(PostgreSQLParser.For_locking_clause_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#for_locking_items}.
	 * @param ctx the parse tree
	 */
	void enterFor_locking_items(PostgreSQLParser.For_locking_itemsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#for_locking_items}.
	 * @param ctx the parse tree
	 */
	void exitFor_locking_items(PostgreSQLParser.For_locking_itemsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#for_locking_item}.
	 * @param ctx the parse tree
	 */
	void enterFor_locking_item(PostgreSQLParser.For_locking_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#for_locking_item}.
	 * @param ctx the parse tree
	 */
	void exitFor_locking_item(PostgreSQLParser.For_locking_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#for_locking_strength}.
	 * @param ctx the parse tree
	 */
	void enterFor_locking_strength(PostgreSQLParser.For_locking_strengthContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#for_locking_strength}.
	 * @param ctx the parse tree
	 */
	void exitFor_locking_strength(PostgreSQLParser.For_locking_strengthContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#locked_rels_list}.
	 * @param ctx the parse tree
	 */
	void enterLocked_rels_list(PostgreSQLParser.Locked_rels_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#locked_rels_list}.
	 * @param ctx the parse tree
	 */
	void exitLocked_rels_list(PostgreSQLParser.Locked_rels_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#values_clause}.
	 * @param ctx the parse tree
	 */
	void enterValues_clause(PostgreSQLParser.Values_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#values_clause}.
	 * @param ctx the parse tree
	 */
	void exitValues_clause(PostgreSQLParser.Values_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#from_clause}.
	 * @param ctx the parse tree
	 */
	void enterFrom_clause(PostgreSQLParser.From_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#from_clause}.
	 * @param ctx the parse tree
	 */
	void exitFrom_clause(PostgreSQLParser.From_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#from_list}.
	 * @param ctx the parse tree
	 */
	void enterFrom_list(PostgreSQLParser.From_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#from_list}.
	 * @param ctx the parse tree
	 */
	void exitFrom_list(PostgreSQLParser.From_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#table_ref}.
	 * @param ctx the parse tree
	 */
	void enterTable_ref(PostgreSQLParser.Table_refContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#table_ref}.
	 * @param ctx the parse tree
	 */
	void exitTable_ref(PostgreSQLParser.Table_refContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#alias_clause}.
	 * @param ctx the parse tree
	 */
	void enterAlias_clause(PostgreSQLParser.Alias_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#alias_clause}.
	 * @param ctx the parse tree
	 */
	void exitAlias_clause(PostgreSQLParser.Alias_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_alias_clause}.
	 * @param ctx the parse tree
	 */
	void enterFunc_alias_clause(PostgreSQLParser.Func_alias_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_alias_clause}.
	 * @param ctx the parse tree
	 */
	void exitFunc_alias_clause(PostgreSQLParser.Func_alias_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#join_type}.
	 * @param ctx the parse tree
	 */
	void enterJoin_type(PostgreSQLParser.Join_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#join_type}.
	 * @param ctx the parse tree
	 */
	void exitJoin_type(PostgreSQLParser.Join_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#join_qual}.
	 * @param ctx the parse tree
	 */
	void enterJoin_qual(PostgreSQLParser.Join_qualContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#join_qual}.
	 * @param ctx the parse tree
	 */
	void exitJoin_qual(PostgreSQLParser.Join_qualContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#relation_expr}.
	 * @param ctx the parse tree
	 */
	void enterRelation_expr(PostgreSQLParser.Relation_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#relation_expr}.
	 * @param ctx the parse tree
	 */
	void exitRelation_expr(PostgreSQLParser.Relation_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#relation_expr_list}.
	 * @param ctx the parse tree
	 */
	void enterRelation_expr_list(PostgreSQLParser.Relation_expr_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#relation_expr_list}.
	 * @param ctx the parse tree
	 */
	void exitRelation_expr_list(PostgreSQLParser.Relation_expr_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#relation_expr_opt_alias}.
	 * @param ctx the parse tree
	 */
	void enterRelation_expr_opt_alias(PostgreSQLParser.Relation_expr_opt_aliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#relation_expr_opt_alias}.
	 * @param ctx the parse tree
	 */
	void exitRelation_expr_opt_alias(PostgreSQLParser.Relation_expr_opt_aliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#tablesample_clause}.
	 * @param ctx the parse tree
	 */
	void enterTablesample_clause(PostgreSQLParser.Tablesample_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#tablesample_clause}.
	 * @param ctx the parse tree
	 */
	void exitTablesample_clause(PostgreSQLParser.Tablesample_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#repeatable_clause_}.
	 * @param ctx the parse tree
	 */
	void enterRepeatable_clause_(PostgreSQLParser.Repeatable_clause_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#repeatable_clause_}.
	 * @param ctx the parse tree
	 */
	void exitRepeatable_clause_(PostgreSQLParser.Repeatable_clause_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_table}.
	 * @param ctx the parse tree
	 */
	void enterFunc_table(PostgreSQLParser.Func_tableContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_table}.
	 * @param ctx the parse tree
	 */
	void exitFunc_table(PostgreSQLParser.Func_tableContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#rowsfrom_item}.
	 * @param ctx the parse tree
	 */
	void enterRowsfrom_item(PostgreSQLParser.Rowsfrom_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#rowsfrom_item}.
	 * @param ctx the parse tree
	 */
	void exitRowsfrom_item(PostgreSQLParser.Rowsfrom_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#rowsfrom_list}.
	 * @param ctx the parse tree
	 */
	void enterRowsfrom_list(PostgreSQLParser.Rowsfrom_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#rowsfrom_list}.
	 * @param ctx the parse tree
	 */
	void exitRowsfrom_list(PostgreSQLParser.Rowsfrom_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#col_def_list_}.
	 * @param ctx the parse tree
	 */
	void enterCol_def_list_(PostgreSQLParser.Col_def_list_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#col_def_list_}.
	 * @param ctx the parse tree
	 */
	void exitCol_def_list_(PostgreSQLParser.Col_def_list_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#ordinality_}.
	 * @param ctx the parse tree
	 */
	void enterOrdinality_(PostgreSQLParser.Ordinality_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#ordinality_}.
	 * @param ctx the parse tree
	 */
	void exitOrdinality_(PostgreSQLParser.Ordinality_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#where_clause}.
	 * @param ctx the parse tree
	 */
	void enterWhere_clause(PostgreSQLParser.Where_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#where_clause}.
	 * @param ctx the parse tree
	 */
	void exitWhere_clause(PostgreSQLParser.Where_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#where_or_current_clause}.
	 * @param ctx the parse tree
	 */
	void enterWhere_or_current_clause(PostgreSQLParser.Where_or_current_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#where_or_current_clause}.
	 * @param ctx the parse tree
	 */
	void exitWhere_or_current_clause(PostgreSQLParser.Where_or_current_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#opttablefuncelementlist}.
	 * @param ctx the parse tree
	 */
	void enterOpttablefuncelementlist(PostgreSQLParser.OpttablefuncelementlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#opttablefuncelementlist}.
	 * @param ctx the parse tree
	 */
	void exitOpttablefuncelementlist(PostgreSQLParser.OpttablefuncelementlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#tablefuncelementlist}.
	 * @param ctx the parse tree
	 */
	void enterTablefuncelementlist(PostgreSQLParser.TablefuncelementlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#tablefuncelementlist}.
	 * @param ctx the parse tree
	 */
	void exitTablefuncelementlist(PostgreSQLParser.TablefuncelementlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#tablefuncelement}.
	 * @param ctx the parse tree
	 */
	void enterTablefuncelement(PostgreSQLParser.TablefuncelementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#tablefuncelement}.
	 * @param ctx the parse tree
	 */
	void exitTablefuncelement(PostgreSQLParser.TablefuncelementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xmltable}.
	 * @param ctx the parse tree
	 */
	void enterXmltable(PostgreSQLParser.XmltableContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xmltable}.
	 * @param ctx the parse tree
	 */
	void exitXmltable(PostgreSQLParser.XmltableContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xmltable_column_list}.
	 * @param ctx the parse tree
	 */
	void enterXmltable_column_list(PostgreSQLParser.Xmltable_column_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xmltable_column_list}.
	 * @param ctx the parse tree
	 */
	void exitXmltable_column_list(PostgreSQLParser.Xmltable_column_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xmltable_column_el}.
	 * @param ctx the parse tree
	 */
	void enterXmltable_column_el(PostgreSQLParser.Xmltable_column_elContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xmltable_column_el}.
	 * @param ctx the parse tree
	 */
	void exitXmltable_column_el(PostgreSQLParser.Xmltable_column_elContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xmltable_column_option_list}.
	 * @param ctx the parse tree
	 */
	void enterXmltable_column_option_list(PostgreSQLParser.Xmltable_column_option_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xmltable_column_option_list}.
	 * @param ctx the parse tree
	 */
	void exitXmltable_column_option_list(PostgreSQLParser.Xmltable_column_option_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xmltable_column_option_el}.
	 * @param ctx the parse tree
	 */
	void enterXmltable_column_option_el(PostgreSQLParser.Xmltable_column_option_elContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xmltable_column_option_el}.
	 * @param ctx the parse tree
	 */
	void exitXmltable_column_option_el(PostgreSQLParser.Xmltable_column_option_elContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xml_namespace_list}.
	 * @param ctx the parse tree
	 */
	void enterXml_namespace_list(PostgreSQLParser.Xml_namespace_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xml_namespace_list}.
	 * @param ctx the parse tree
	 */
	void exitXml_namespace_list(PostgreSQLParser.Xml_namespace_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xml_namespace_el}.
	 * @param ctx the parse tree
	 */
	void enterXml_namespace_el(PostgreSQLParser.Xml_namespace_elContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xml_namespace_el}.
	 * @param ctx the parse tree
	 */
	void exitXml_namespace_el(PostgreSQLParser.Xml_namespace_elContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#typename}.
	 * @param ctx the parse tree
	 */
	void enterTypename(PostgreSQLParser.TypenameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#typename}.
	 * @param ctx the parse tree
	 */
	void exitTypename(PostgreSQLParser.TypenameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#opt_array_bounds}.
	 * @param ctx the parse tree
	 */
	void enterOpt_array_bounds(PostgreSQLParser.Opt_array_boundsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#opt_array_bounds}.
	 * @param ctx the parse tree
	 */
	void exitOpt_array_bounds(PostgreSQLParser.Opt_array_boundsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#simpletypename}.
	 * @param ctx the parse tree
	 */
	void enterSimpletypename(PostgreSQLParser.SimpletypenameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#simpletypename}.
	 * @param ctx the parse tree
	 */
	void exitSimpletypename(PostgreSQLParser.SimpletypenameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#consttypename}.
	 * @param ctx the parse tree
	 */
	void enterConsttypename(PostgreSQLParser.ConsttypenameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#consttypename}.
	 * @param ctx the parse tree
	 */
	void exitConsttypename(PostgreSQLParser.ConsttypenameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#generictype}.
	 * @param ctx the parse tree
	 */
	void enterGenerictype(PostgreSQLParser.GenerictypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#generictype}.
	 * @param ctx the parse tree
	 */
	void exitGenerictype(PostgreSQLParser.GenerictypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#type_modifiers_}.
	 * @param ctx the parse tree
	 */
	void enterType_modifiers_(PostgreSQLParser.Type_modifiers_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#type_modifiers_}.
	 * @param ctx the parse tree
	 */
	void exitType_modifiers_(PostgreSQLParser.Type_modifiers_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#numeric}.
	 * @param ctx the parse tree
	 */
	void enterNumeric(PostgreSQLParser.NumericContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#numeric}.
	 * @param ctx the parse tree
	 */
	void exitNumeric(PostgreSQLParser.NumericContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#float_}.
	 * @param ctx the parse tree
	 */
	void enterFloat_(PostgreSQLParser.Float_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#float_}.
	 * @param ctx the parse tree
	 */
	void exitFloat_(PostgreSQLParser.Float_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#bit}.
	 * @param ctx the parse tree
	 */
	void enterBit(PostgreSQLParser.BitContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#bit}.
	 * @param ctx the parse tree
	 */
	void exitBit(PostgreSQLParser.BitContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#constbit}.
	 * @param ctx the parse tree
	 */
	void enterConstbit(PostgreSQLParser.ConstbitContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#constbit}.
	 * @param ctx the parse tree
	 */
	void exitConstbit(PostgreSQLParser.ConstbitContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#bitwithlength}.
	 * @param ctx the parse tree
	 */
	void enterBitwithlength(PostgreSQLParser.BitwithlengthContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#bitwithlength}.
	 * @param ctx the parse tree
	 */
	void exitBitwithlength(PostgreSQLParser.BitwithlengthContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#bitwithoutlength}.
	 * @param ctx the parse tree
	 */
	void enterBitwithoutlength(PostgreSQLParser.BitwithoutlengthContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#bitwithoutlength}.
	 * @param ctx the parse tree
	 */
	void exitBitwithoutlength(PostgreSQLParser.BitwithoutlengthContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#character}.
	 * @param ctx the parse tree
	 */
	void enterCharacter(PostgreSQLParser.CharacterContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#character}.
	 * @param ctx the parse tree
	 */
	void exitCharacter(PostgreSQLParser.CharacterContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#constcharacter}.
	 * @param ctx the parse tree
	 */
	void enterConstcharacter(PostgreSQLParser.ConstcharacterContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#constcharacter}.
	 * @param ctx the parse tree
	 */
	void exitConstcharacter(PostgreSQLParser.ConstcharacterContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#character_c}.
	 * @param ctx the parse tree
	 */
	void enterCharacter_c(PostgreSQLParser.Character_cContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#character_c}.
	 * @param ctx the parse tree
	 */
	void exitCharacter_c(PostgreSQLParser.Character_cContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#varying_}.
	 * @param ctx the parse tree
	 */
	void enterVarying_(PostgreSQLParser.Varying_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#varying_}.
	 * @param ctx the parse tree
	 */
	void exitVarying_(PostgreSQLParser.Varying_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#constdatetime}.
	 * @param ctx the parse tree
	 */
	void enterConstdatetime(PostgreSQLParser.ConstdatetimeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#constdatetime}.
	 * @param ctx the parse tree
	 */
	void exitConstdatetime(PostgreSQLParser.ConstdatetimeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#constinterval}.
	 * @param ctx the parse tree
	 */
	void enterConstinterval(PostgreSQLParser.ConstintervalContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#constinterval}.
	 * @param ctx the parse tree
	 */
	void exitConstinterval(PostgreSQLParser.ConstintervalContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#timezone_}.
	 * @param ctx the parse tree
	 */
	void enterTimezone_(PostgreSQLParser.Timezone_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#timezone_}.
	 * @param ctx the parse tree
	 */
	void exitTimezone_(PostgreSQLParser.Timezone_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#interval_}.
	 * @param ctx the parse tree
	 */
	void enterInterval_(PostgreSQLParser.Interval_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#interval_}.
	 * @param ctx the parse tree
	 */
	void exitInterval_(PostgreSQLParser.Interval_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#interval_second}.
	 * @param ctx the parse tree
	 */
	void enterInterval_second(PostgreSQLParser.Interval_secondContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#interval_second}.
	 * @param ctx the parse tree
	 */
	void exitInterval_second(PostgreSQLParser.Interval_secondContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#jsonType}.
	 * @param ctx the parse tree
	 */
	void enterJsonType(PostgreSQLParser.JsonTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#jsonType}.
	 * @param ctx the parse tree
	 */
	void exitJsonType(PostgreSQLParser.JsonTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#escape_}.
	 * @param ctx the parse tree
	 */
	void enterEscape_(PostgreSQLParser.Escape_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#escape_}.
	 * @param ctx the parse tree
	 */
	void exitEscape_(PostgreSQLParser.Escape_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr}.
	 * @param ctx the parse tree
	 */
	void enterA_expr(PostgreSQLParser.A_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr}.
	 * @param ctx the parse tree
	 */
	void exitA_expr(PostgreSQLParser.A_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_qual}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_qual(PostgreSQLParser.A_expr_qualContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_qual}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_qual(PostgreSQLParser.A_expr_qualContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_lessless}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_lessless(PostgreSQLParser.A_expr_lesslessContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_lessless}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_lessless(PostgreSQLParser.A_expr_lesslessContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_or}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_or(PostgreSQLParser.A_expr_orContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_or}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_or(PostgreSQLParser.A_expr_orContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_and}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_and(PostgreSQLParser.A_expr_andContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_and}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_and(PostgreSQLParser.A_expr_andContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_between}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_between(PostgreSQLParser.A_expr_betweenContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_between}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_between(PostgreSQLParser.A_expr_betweenContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_in}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_in(PostgreSQLParser.A_expr_inContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_in}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_in(PostgreSQLParser.A_expr_inContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_unary_not}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_unary_not(PostgreSQLParser.A_expr_unary_notContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_unary_not}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_unary_not(PostgreSQLParser.A_expr_unary_notContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_isnull}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_isnull(PostgreSQLParser.A_expr_isnullContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_isnull}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_isnull(PostgreSQLParser.A_expr_isnullContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_is_not}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_is_not(PostgreSQLParser.A_expr_is_notContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_is_not}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_is_not(PostgreSQLParser.A_expr_is_notContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_compare}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_compare(PostgreSQLParser.A_expr_compareContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_compare}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_compare(PostgreSQLParser.A_expr_compareContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_like}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_like(PostgreSQLParser.A_expr_likeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_like}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_like(PostgreSQLParser.A_expr_likeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_qual_op}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_qual_op(PostgreSQLParser.A_expr_qual_opContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_qual_op}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_qual_op(PostgreSQLParser.A_expr_qual_opContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_unary_qualop}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_unary_qualop(PostgreSQLParser.A_expr_unary_qualopContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_unary_qualop}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_unary_qualop(PostgreSQLParser.A_expr_unary_qualopContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_add}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_add(PostgreSQLParser.A_expr_addContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_add}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_add(PostgreSQLParser.A_expr_addContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_mul}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_mul(PostgreSQLParser.A_expr_mulContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_mul}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_mul(PostgreSQLParser.A_expr_mulContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_caret}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_caret(PostgreSQLParser.A_expr_caretContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_caret}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_caret(PostgreSQLParser.A_expr_caretContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_unary_sign}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_unary_sign(PostgreSQLParser.A_expr_unary_signContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_unary_sign}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_unary_sign(PostgreSQLParser.A_expr_unary_signContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_at_time_zone}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_at_time_zone(PostgreSQLParser.A_expr_at_time_zoneContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_at_time_zone}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_at_time_zone(PostgreSQLParser.A_expr_at_time_zoneContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_collate}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_collate(PostgreSQLParser.A_expr_collateContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_collate}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_collate(PostgreSQLParser.A_expr_collateContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#a_expr_typecast}.
	 * @param ctx the parse tree
	 */
	void enterA_expr_typecast(PostgreSQLParser.A_expr_typecastContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#a_expr_typecast}.
	 * @param ctx the parse tree
	 */
	void exitA_expr_typecast(PostgreSQLParser.A_expr_typecastContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#b_expr}.
	 * @param ctx the parse tree
	 */
	void enterB_expr(PostgreSQLParser.B_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#b_expr}.
	 * @param ctx the parse tree
	 */
	void exitB_expr(PostgreSQLParser.B_exprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code c_expr_exists}
	 * labeled alternative in {@link PostgreSQLParser#c_expr}.
	 * @param ctx the parse tree
	 */
	void enterC_expr_exists(PostgreSQLParser.C_expr_existsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code c_expr_exists}
	 * labeled alternative in {@link PostgreSQLParser#c_expr}.
	 * @param ctx the parse tree
	 */
	void exitC_expr_exists(PostgreSQLParser.C_expr_existsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code c_expr_expr}
	 * labeled alternative in {@link PostgreSQLParser#c_expr}.
	 * @param ctx the parse tree
	 */
	void enterC_expr_expr(PostgreSQLParser.C_expr_exprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code c_expr_expr}
	 * labeled alternative in {@link PostgreSQLParser#c_expr}.
	 * @param ctx the parse tree
	 */
	void exitC_expr_expr(PostgreSQLParser.C_expr_exprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code c_expr_case}
	 * labeled alternative in {@link PostgreSQLParser#c_expr}.
	 * @param ctx the parse tree
	 */
	void enterC_expr_case(PostgreSQLParser.C_expr_caseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code c_expr_case}
	 * labeled alternative in {@link PostgreSQLParser#c_expr}.
	 * @param ctx the parse tree
	 */
	void exitC_expr_case(PostgreSQLParser.C_expr_caseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#plsqlvariablename}.
	 * @param ctx the parse tree
	 */
	void enterPlsqlvariablename(PostgreSQLParser.PlsqlvariablenameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#plsqlvariablename}.
	 * @param ctx the parse tree
	 */
	void exitPlsqlvariablename(PostgreSQLParser.PlsqlvariablenameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_application}.
	 * @param ctx the parse tree
	 */
	void enterFunc_application(PostgreSQLParser.Func_applicationContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_application}.
	 * @param ctx the parse tree
	 */
	void exitFunc_application(PostgreSQLParser.Func_applicationContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_expr}.
	 * @param ctx the parse tree
	 */
	void enterFunc_expr(PostgreSQLParser.Func_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_expr}.
	 * @param ctx the parse tree
	 */
	void exitFunc_expr(PostgreSQLParser.Func_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_expr_windowless}.
	 * @param ctx the parse tree
	 */
	void enterFunc_expr_windowless(PostgreSQLParser.Func_expr_windowlessContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_expr_windowless}.
	 * @param ctx the parse tree
	 */
	void exitFunc_expr_windowless(PostgreSQLParser.Func_expr_windowlessContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_expr_common_subexpr}.
	 * @param ctx the parse tree
	 */
	void enterFunc_expr_common_subexpr(PostgreSQLParser.Func_expr_common_subexprContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_expr_common_subexpr}.
	 * @param ctx the parse tree
	 */
	void exitFunc_expr_common_subexpr(PostgreSQLParser.Func_expr_common_subexprContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xml_root_version}.
	 * @param ctx the parse tree
	 */
	void enterXml_root_version(PostgreSQLParser.Xml_root_versionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xml_root_version}.
	 * @param ctx the parse tree
	 */
	void exitXml_root_version(PostgreSQLParser.Xml_root_versionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xml_root_standalone_}.
	 * @param ctx the parse tree
	 */
	void enterXml_root_standalone_(PostgreSQLParser.Xml_root_standalone_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xml_root_standalone_}.
	 * @param ctx the parse tree
	 */
	void exitXml_root_standalone_(PostgreSQLParser.Xml_root_standalone_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xml_attributes}.
	 * @param ctx the parse tree
	 */
	void enterXml_attributes(PostgreSQLParser.Xml_attributesContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xml_attributes}.
	 * @param ctx the parse tree
	 */
	void exitXml_attributes(PostgreSQLParser.Xml_attributesContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xml_attribute_list}.
	 * @param ctx the parse tree
	 */
	void enterXml_attribute_list(PostgreSQLParser.Xml_attribute_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xml_attribute_list}.
	 * @param ctx the parse tree
	 */
	void exitXml_attribute_list(PostgreSQLParser.Xml_attribute_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xml_attribute_el}.
	 * @param ctx the parse tree
	 */
	void enterXml_attribute_el(PostgreSQLParser.Xml_attribute_elContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xml_attribute_el}.
	 * @param ctx the parse tree
	 */
	void exitXml_attribute_el(PostgreSQLParser.Xml_attribute_elContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#document_or_content}.
	 * @param ctx the parse tree
	 */
	void enterDocument_or_content(PostgreSQLParser.Document_or_contentContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#document_or_content}.
	 * @param ctx the parse tree
	 */
	void exitDocument_or_content(PostgreSQLParser.Document_or_contentContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xml_whitespace_option}.
	 * @param ctx the parse tree
	 */
	void enterXml_whitespace_option(PostgreSQLParser.Xml_whitespace_optionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xml_whitespace_option}.
	 * @param ctx the parse tree
	 */
	void exitXml_whitespace_option(PostgreSQLParser.Xml_whitespace_optionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xmlexists_argument}.
	 * @param ctx the parse tree
	 */
	void enterXmlexists_argument(PostgreSQLParser.Xmlexists_argumentContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xmlexists_argument}.
	 * @param ctx the parse tree
	 */
	void exitXmlexists_argument(PostgreSQLParser.Xmlexists_argumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xml_passing_mech}.
	 * @param ctx the parse tree
	 */
	void enterXml_passing_mech(PostgreSQLParser.Xml_passing_mechContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xml_passing_mech}.
	 * @param ctx the parse tree
	 */
	void exitXml_passing_mech(PostgreSQLParser.Xml_passing_mechContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#within_group_clause}.
	 * @param ctx the parse tree
	 */
	void enterWithin_group_clause(PostgreSQLParser.Within_group_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#within_group_clause}.
	 * @param ctx the parse tree
	 */
	void exitWithin_group_clause(PostgreSQLParser.Within_group_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#filter_clause}.
	 * @param ctx the parse tree
	 */
	void enterFilter_clause(PostgreSQLParser.Filter_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#filter_clause}.
	 * @param ctx the parse tree
	 */
	void exitFilter_clause(PostgreSQLParser.Filter_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#window_clause}.
	 * @param ctx the parse tree
	 */
	void enterWindow_clause(PostgreSQLParser.Window_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#window_clause}.
	 * @param ctx the parse tree
	 */
	void exitWindow_clause(PostgreSQLParser.Window_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#window_definition_list}.
	 * @param ctx the parse tree
	 */
	void enterWindow_definition_list(PostgreSQLParser.Window_definition_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#window_definition_list}.
	 * @param ctx the parse tree
	 */
	void exitWindow_definition_list(PostgreSQLParser.Window_definition_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#window_definition}.
	 * @param ctx the parse tree
	 */
	void enterWindow_definition(PostgreSQLParser.Window_definitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#window_definition}.
	 * @param ctx the parse tree
	 */
	void exitWindow_definition(PostgreSQLParser.Window_definitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#over_clause}.
	 * @param ctx the parse tree
	 */
	void enterOver_clause(PostgreSQLParser.Over_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#over_clause}.
	 * @param ctx the parse tree
	 */
	void exitOver_clause(PostgreSQLParser.Over_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#window_specification}.
	 * @param ctx the parse tree
	 */
	void enterWindow_specification(PostgreSQLParser.Window_specificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#window_specification}.
	 * @param ctx the parse tree
	 */
	void exitWindow_specification(PostgreSQLParser.Window_specificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#existing_window_name_}.
	 * @param ctx the parse tree
	 */
	void enterExisting_window_name_(PostgreSQLParser.Existing_window_name_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#existing_window_name_}.
	 * @param ctx the parse tree
	 */
	void exitExisting_window_name_(PostgreSQLParser.Existing_window_name_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#partition_clause_}.
	 * @param ctx the parse tree
	 */
	void enterPartition_clause_(PostgreSQLParser.Partition_clause_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#partition_clause_}.
	 * @param ctx the parse tree
	 */
	void exitPartition_clause_(PostgreSQLParser.Partition_clause_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#frame_clause_}.
	 * @param ctx the parse tree
	 */
	void enterFrame_clause_(PostgreSQLParser.Frame_clause_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#frame_clause_}.
	 * @param ctx the parse tree
	 */
	void exitFrame_clause_(PostgreSQLParser.Frame_clause_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#frame_extent}.
	 * @param ctx the parse tree
	 */
	void enterFrame_extent(PostgreSQLParser.Frame_extentContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#frame_extent}.
	 * @param ctx the parse tree
	 */
	void exitFrame_extent(PostgreSQLParser.Frame_extentContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#frame_bound}.
	 * @param ctx the parse tree
	 */
	void enterFrame_bound(PostgreSQLParser.Frame_boundContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#frame_bound}.
	 * @param ctx the parse tree
	 */
	void exitFrame_bound(PostgreSQLParser.Frame_boundContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#window_exclusion_clause_}.
	 * @param ctx the parse tree
	 */
	void enterWindow_exclusion_clause_(PostgreSQLParser.Window_exclusion_clause_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#window_exclusion_clause_}.
	 * @param ctx the parse tree
	 */
	void exitWindow_exclusion_clause_(PostgreSQLParser.Window_exclusion_clause_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#row}.
	 * @param ctx the parse tree
	 */
	void enterRow(PostgreSQLParser.RowContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#row}.
	 * @param ctx the parse tree
	 */
	void exitRow(PostgreSQLParser.RowContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#explicit_row}.
	 * @param ctx the parse tree
	 */
	void enterExplicit_row(PostgreSQLParser.Explicit_rowContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#explicit_row}.
	 * @param ctx the parse tree
	 */
	void exitExplicit_row(PostgreSQLParser.Explicit_rowContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#implicit_row}.
	 * @param ctx the parse tree
	 */
	void enterImplicit_row(PostgreSQLParser.Implicit_rowContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#implicit_row}.
	 * @param ctx the parse tree
	 */
	void exitImplicit_row(PostgreSQLParser.Implicit_rowContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#sub_type}.
	 * @param ctx the parse tree
	 */
	void enterSub_type(PostgreSQLParser.Sub_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#sub_type}.
	 * @param ctx the parse tree
	 */
	void exitSub_type(PostgreSQLParser.Sub_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#all_op}.
	 * @param ctx the parse tree
	 */
	void enterAll_op(PostgreSQLParser.All_opContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#all_op}.
	 * @param ctx the parse tree
	 */
	void exitAll_op(PostgreSQLParser.All_opContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#mathop}.
	 * @param ctx the parse tree
	 */
	void enterMathop(PostgreSQLParser.MathopContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#mathop}.
	 * @param ctx the parse tree
	 */
	void exitMathop(PostgreSQLParser.MathopContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#qual_op}.
	 * @param ctx the parse tree
	 */
	void enterQual_op(PostgreSQLParser.Qual_opContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#qual_op}.
	 * @param ctx the parse tree
	 */
	void exitQual_op(PostgreSQLParser.Qual_opContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#qual_all_op}.
	 * @param ctx the parse tree
	 */
	void enterQual_all_op(PostgreSQLParser.Qual_all_opContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#qual_all_op}.
	 * @param ctx the parse tree
	 */
	void exitQual_all_op(PostgreSQLParser.Qual_all_opContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#subquery_Op}.
	 * @param ctx the parse tree
	 */
	void enterSubquery_Op(PostgreSQLParser.Subquery_OpContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#subquery_Op}.
	 * @param ctx the parse tree
	 */
	void exitSubquery_Op(PostgreSQLParser.Subquery_OpContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#expr_list}.
	 * @param ctx the parse tree
	 */
	void enterExpr_list(PostgreSQLParser.Expr_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#expr_list}.
	 * @param ctx the parse tree
	 */
	void exitExpr_list(PostgreSQLParser.Expr_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_arg_list}.
	 * @param ctx the parse tree
	 */
	void enterFunc_arg_list(PostgreSQLParser.Func_arg_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_arg_list}.
	 * @param ctx the parse tree
	 */
	void exitFunc_arg_list(PostgreSQLParser.Func_arg_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_arg_expr}.
	 * @param ctx the parse tree
	 */
	void enterFunc_arg_expr(PostgreSQLParser.Func_arg_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_arg_expr}.
	 * @param ctx the parse tree
	 */
	void exitFunc_arg_expr(PostgreSQLParser.Func_arg_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#type_list}.
	 * @param ctx the parse tree
	 */
	void enterType_list(PostgreSQLParser.Type_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#type_list}.
	 * @param ctx the parse tree
	 */
	void exitType_list(PostgreSQLParser.Type_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#array_expr}.
	 * @param ctx the parse tree
	 */
	void enterArray_expr(PostgreSQLParser.Array_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#array_expr}.
	 * @param ctx the parse tree
	 */
	void exitArray_expr(PostgreSQLParser.Array_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#array_expr_list}.
	 * @param ctx the parse tree
	 */
	void enterArray_expr_list(PostgreSQLParser.Array_expr_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#array_expr_list}.
	 * @param ctx the parse tree
	 */
	void exitArray_expr_list(PostgreSQLParser.Array_expr_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#extract_list}.
	 * @param ctx the parse tree
	 */
	void enterExtract_list(PostgreSQLParser.Extract_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#extract_list}.
	 * @param ctx the parse tree
	 */
	void exitExtract_list(PostgreSQLParser.Extract_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#extract_arg}.
	 * @param ctx the parse tree
	 */
	void enterExtract_arg(PostgreSQLParser.Extract_argContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#extract_arg}.
	 * @param ctx the parse tree
	 */
	void exitExtract_arg(PostgreSQLParser.Extract_argContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#unicode_normal_form}.
	 * @param ctx the parse tree
	 */
	void enterUnicode_normal_form(PostgreSQLParser.Unicode_normal_formContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#unicode_normal_form}.
	 * @param ctx the parse tree
	 */
	void exitUnicode_normal_form(PostgreSQLParser.Unicode_normal_formContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#overlay_list}.
	 * @param ctx the parse tree
	 */
	void enterOverlay_list(PostgreSQLParser.Overlay_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#overlay_list}.
	 * @param ctx the parse tree
	 */
	void exitOverlay_list(PostgreSQLParser.Overlay_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#position_list}.
	 * @param ctx the parse tree
	 */
	void enterPosition_list(PostgreSQLParser.Position_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#position_list}.
	 * @param ctx the parse tree
	 */
	void exitPosition_list(PostgreSQLParser.Position_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#substr_list}.
	 * @param ctx the parse tree
	 */
	void enterSubstr_list(PostgreSQLParser.Substr_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#substr_list}.
	 * @param ctx the parse tree
	 */
	void exitSubstr_list(PostgreSQLParser.Substr_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#trim_list}.
	 * @param ctx the parse tree
	 */
	void enterTrim_list(PostgreSQLParser.Trim_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#trim_list}.
	 * @param ctx the parse tree
	 */
	void exitTrim_list(PostgreSQLParser.Trim_listContext ctx);
	/**
	 * Enter a parse tree produced by the {@code in_expr_select}
	 * labeled alternative in {@link PostgreSQLParser#in_expr}.
	 * @param ctx the parse tree
	 */
	void enterIn_expr_select(PostgreSQLParser.In_expr_selectContext ctx);
	/**
	 * Exit a parse tree produced by the {@code in_expr_select}
	 * labeled alternative in {@link PostgreSQLParser#in_expr}.
	 * @param ctx the parse tree
	 */
	void exitIn_expr_select(PostgreSQLParser.In_expr_selectContext ctx);
	/**
	 * Enter a parse tree produced by the {@code in_expr_list}
	 * labeled alternative in {@link PostgreSQLParser#in_expr}.
	 * @param ctx the parse tree
	 */
	void enterIn_expr_list(PostgreSQLParser.In_expr_listContext ctx);
	/**
	 * Exit a parse tree produced by the {@code in_expr_list}
	 * labeled alternative in {@link PostgreSQLParser#in_expr}.
	 * @param ctx the parse tree
	 */
	void exitIn_expr_list(PostgreSQLParser.In_expr_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#case_expr}.
	 * @param ctx the parse tree
	 */
	void enterCase_expr(PostgreSQLParser.Case_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#case_expr}.
	 * @param ctx the parse tree
	 */
	void exitCase_expr(PostgreSQLParser.Case_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#when_clause_list}.
	 * @param ctx the parse tree
	 */
	void enterWhen_clause_list(PostgreSQLParser.When_clause_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#when_clause_list}.
	 * @param ctx the parse tree
	 */
	void exitWhen_clause_list(PostgreSQLParser.When_clause_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#when_clause}.
	 * @param ctx the parse tree
	 */
	void enterWhen_clause(PostgreSQLParser.When_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#when_clause}.
	 * @param ctx the parse tree
	 */
	void exitWhen_clause(PostgreSQLParser.When_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#case_default}.
	 * @param ctx the parse tree
	 */
	void enterCase_default(PostgreSQLParser.Case_defaultContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#case_default}.
	 * @param ctx the parse tree
	 */
	void exitCase_default(PostgreSQLParser.Case_defaultContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#case_arg}.
	 * @param ctx the parse tree
	 */
	void enterCase_arg(PostgreSQLParser.Case_argContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#case_arg}.
	 * @param ctx the parse tree
	 */
	void exitCase_arg(PostgreSQLParser.Case_argContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#columnref}.
	 * @param ctx the parse tree
	 */
	void enterColumnref(PostgreSQLParser.ColumnrefContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#columnref}.
	 * @param ctx the parse tree
	 */
	void exitColumnref(PostgreSQLParser.ColumnrefContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#indirection_el}.
	 * @param ctx the parse tree
	 */
	void enterIndirection_el(PostgreSQLParser.Indirection_elContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#indirection_el}.
	 * @param ctx the parse tree
	 */
	void exitIndirection_el(PostgreSQLParser.Indirection_elContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#slice_bound_}.
	 * @param ctx the parse tree
	 */
	void enterSlice_bound_(PostgreSQLParser.Slice_bound_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#slice_bound_}.
	 * @param ctx the parse tree
	 */
	void exitSlice_bound_(PostgreSQLParser.Slice_bound_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#indirection}.
	 * @param ctx the parse tree
	 */
	void enterIndirection(PostgreSQLParser.IndirectionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#indirection}.
	 * @param ctx the parse tree
	 */
	void exitIndirection(PostgreSQLParser.IndirectionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#opt_indirection}.
	 * @param ctx the parse tree
	 */
	void enterOpt_indirection(PostgreSQLParser.Opt_indirectionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#opt_indirection}.
	 * @param ctx the parse tree
	 */
	void exitOpt_indirection(PostgreSQLParser.Opt_indirectionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_passing_clause}.
	 * @param ctx the parse tree
	 */
	void enterJson_passing_clause(PostgreSQLParser.Json_passing_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_passing_clause}.
	 * @param ctx the parse tree
	 */
	void exitJson_passing_clause(PostgreSQLParser.Json_passing_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_arguments}.
	 * @param ctx the parse tree
	 */
	void enterJson_arguments(PostgreSQLParser.Json_argumentsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_arguments}.
	 * @param ctx the parse tree
	 */
	void exitJson_arguments(PostgreSQLParser.Json_argumentsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_argument}.
	 * @param ctx the parse tree
	 */
	void enterJson_argument(PostgreSQLParser.Json_argumentContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_argument}.
	 * @param ctx the parse tree
	 */
	void exitJson_argument(PostgreSQLParser.Json_argumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_wrapper_behavior}.
	 * @param ctx the parse tree
	 */
	void enterJson_wrapper_behavior(PostgreSQLParser.Json_wrapper_behaviorContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_wrapper_behavior}.
	 * @param ctx the parse tree
	 */
	void exitJson_wrapper_behavior(PostgreSQLParser.Json_wrapper_behaviorContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_behavior}.
	 * @param ctx the parse tree
	 */
	void enterJson_behavior(PostgreSQLParser.Json_behaviorContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_behavior}.
	 * @param ctx the parse tree
	 */
	void exitJson_behavior(PostgreSQLParser.Json_behaviorContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_behavior_type}.
	 * @param ctx the parse tree
	 */
	void enterJson_behavior_type(PostgreSQLParser.Json_behavior_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_behavior_type}.
	 * @param ctx the parse tree
	 */
	void exitJson_behavior_type(PostgreSQLParser.Json_behavior_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_behavior_clause}.
	 * @param ctx the parse tree
	 */
	void enterJson_behavior_clause(PostgreSQLParser.Json_behavior_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_behavior_clause}.
	 * @param ctx the parse tree
	 */
	void exitJson_behavior_clause(PostgreSQLParser.Json_behavior_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_on_error_clause}.
	 * @param ctx the parse tree
	 */
	void enterJson_on_error_clause(PostgreSQLParser.Json_on_error_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_on_error_clause}.
	 * @param ctx the parse tree
	 */
	void exitJson_on_error_clause(PostgreSQLParser.Json_on_error_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_value_expr}.
	 * @param ctx the parse tree
	 */
	void enterJson_value_expr(PostgreSQLParser.Json_value_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_value_expr}.
	 * @param ctx the parse tree
	 */
	void exitJson_value_expr(PostgreSQLParser.Json_value_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_format_clause}.
	 * @param ctx the parse tree
	 */
	void enterJson_format_clause(PostgreSQLParser.Json_format_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_format_clause}.
	 * @param ctx the parse tree
	 */
	void exitJson_format_clause(PostgreSQLParser.Json_format_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_quotes_clause}.
	 * @param ctx the parse tree
	 */
	void enterJson_quotes_clause(PostgreSQLParser.Json_quotes_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_quotes_clause}.
	 * @param ctx the parse tree
	 */
	void exitJson_quotes_clause(PostgreSQLParser.Json_quotes_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_returning_clause}.
	 * @param ctx the parse tree
	 */
	void enterJson_returning_clause(PostgreSQLParser.Json_returning_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_returning_clause}.
	 * @param ctx the parse tree
	 */
	void exitJson_returning_clause(PostgreSQLParser.Json_returning_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_predicate_type_constraint}.
	 * @param ctx the parse tree
	 */
	void enterJson_predicate_type_constraint(PostgreSQLParser.Json_predicate_type_constraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_predicate_type_constraint}.
	 * @param ctx the parse tree
	 */
	void exitJson_predicate_type_constraint(PostgreSQLParser.Json_predicate_type_constraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_key_uniqueness_constraint}.
	 * @param ctx the parse tree
	 */
	void enterJson_key_uniqueness_constraint(PostgreSQLParser.Json_key_uniqueness_constraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_key_uniqueness_constraint}.
	 * @param ctx the parse tree
	 */
	void exitJson_key_uniqueness_constraint(PostgreSQLParser.Json_key_uniqueness_constraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_name_and_value_list}.
	 * @param ctx the parse tree
	 */
	void enterJson_name_and_value_list(PostgreSQLParser.Json_name_and_value_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_name_and_value_list}.
	 * @param ctx the parse tree
	 */
	void exitJson_name_and_value_list(PostgreSQLParser.Json_name_and_value_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_name_and_value}.
	 * @param ctx the parse tree
	 */
	void enterJson_name_and_value(PostgreSQLParser.Json_name_and_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_name_and_value}.
	 * @param ctx the parse tree
	 */
	void exitJson_name_and_value(PostgreSQLParser.Json_name_and_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_object_constructor_null_clause}.
	 * @param ctx the parse tree
	 */
	void enterJson_object_constructor_null_clause(PostgreSQLParser.Json_object_constructor_null_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_object_constructor_null_clause}.
	 * @param ctx the parse tree
	 */
	void exitJson_object_constructor_null_clause(PostgreSQLParser.Json_object_constructor_null_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_array_constructor_null_clause}.
	 * @param ctx the parse tree
	 */
	void enterJson_array_constructor_null_clause(PostgreSQLParser.Json_array_constructor_null_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_array_constructor_null_clause}.
	 * @param ctx the parse tree
	 */
	void exitJson_array_constructor_null_clause(PostgreSQLParser.Json_array_constructor_null_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_value_expr_list}.
	 * @param ctx the parse tree
	 */
	void enterJson_value_expr_list(PostgreSQLParser.Json_value_expr_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_value_expr_list}.
	 * @param ctx the parse tree
	 */
	void exitJson_value_expr_list(PostgreSQLParser.Json_value_expr_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_aggregate_func}.
	 * @param ctx the parse tree
	 */
	void enterJson_aggregate_func(PostgreSQLParser.Json_aggregate_funcContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_aggregate_func}.
	 * @param ctx the parse tree
	 */
	void exitJson_aggregate_func(PostgreSQLParser.Json_aggregate_funcContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#json_array_aggregate_order_by_clause}.
	 * @param ctx the parse tree
	 */
	void enterJson_array_aggregate_order_by_clause(PostgreSQLParser.Json_array_aggregate_order_by_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#json_array_aggregate_order_by_clause}.
	 * @param ctx the parse tree
	 */
	void exitJson_array_aggregate_order_by_clause(PostgreSQLParser.Json_array_aggregate_order_by_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#target_list_}.
	 * @param ctx the parse tree
	 */
	void enterTarget_list_(PostgreSQLParser.Target_list_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#target_list_}.
	 * @param ctx the parse tree
	 */
	void exitTarget_list_(PostgreSQLParser.Target_list_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#target_list}.
	 * @param ctx the parse tree
	 */
	void enterTarget_list(PostgreSQLParser.Target_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#target_list}.
	 * @param ctx the parse tree
	 */
	void exitTarget_list(PostgreSQLParser.Target_listContext ctx);
	/**
	 * Enter a parse tree produced by the {@code target_label}
	 * labeled alternative in {@link PostgreSQLParser#target_el}.
	 * @param ctx the parse tree
	 */
	void enterTarget_label(PostgreSQLParser.Target_labelContext ctx);
	/**
	 * Exit a parse tree produced by the {@code target_label}
	 * labeled alternative in {@link PostgreSQLParser#target_el}.
	 * @param ctx the parse tree
	 */
	void exitTarget_label(PostgreSQLParser.Target_labelContext ctx);
	/**
	 * Enter a parse tree produced by the {@code target_star}
	 * labeled alternative in {@link PostgreSQLParser#target_el}.
	 * @param ctx the parse tree
	 */
	void enterTarget_star(PostgreSQLParser.Target_starContext ctx);
	/**
	 * Exit a parse tree produced by the {@code target_star}
	 * labeled alternative in {@link PostgreSQLParser#target_el}.
	 * @param ctx the parse tree
	 */
	void exitTarget_star(PostgreSQLParser.Target_starContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#qualified_name_list}.
	 * @param ctx the parse tree
	 */
	void enterQualified_name_list(PostgreSQLParser.Qualified_name_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#qualified_name_list}.
	 * @param ctx the parse tree
	 */
	void exitQualified_name_list(PostgreSQLParser.Qualified_name_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#qualified_name}.
	 * @param ctx the parse tree
	 */
	void enterQualified_name(PostgreSQLParser.Qualified_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#qualified_name}.
	 * @param ctx the parse tree
	 */
	void exitQualified_name(PostgreSQLParser.Qualified_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#name_list}.
	 * @param ctx the parse tree
	 */
	void enterName_list(PostgreSQLParser.Name_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#name_list}.
	 * @param ctx the parse tree
	 */
	void exitName_list(PostgreSQLParser.Name_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#name}.
	 * @param ctx the parse tree
	 */
	void enterName(PostgreSQLParser.NameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#name}.
	 * @param ctx the parse tree
	 */
	void exitName(PostgreSQLParser.NameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#attr_name}.
	 * @param ctx the parse tree
	 */
	void enterAttr_name(PostgreSQLParser.Attr_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#attr_name}.
	 * @param ctx the parse tree
	 */
	void exitAttr_name(PostgreSQLParser.Attr_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#file_name}.
	 * @param ctx the parse tree
	 */
	void enterFile_name(PostgreSQLParser.File_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#file_name}.
	 * @param ctx the parse tree
	 */
	void exitFile_name(PostgreSQLParser.File_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#func_name}.
	 * @param ctx the parse tree
	 */
	void enterFunc_name(PostgreSQLParser.Func_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#func_name}.
	 * @param ctx the parse tree
	 */
	void exitFunc_name(PostgreSQLParser.Func_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#aexprconst}.
	 * @param ctx the parse tree
	 */
	void enterAexprconst(PostgreSQLParser.AexprconstContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#aexprconst}.
	 * @param ctx the parse tree
	 */
	void exitAexprconst(PostgreSQLParser.AexprconstContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#xconst}.
	 * @param ctx the parse tree
	 */
	void enterXconst(PostgreSQLParser.XconstContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#xconst}.
	 * @param ctx the parse tree
	 */
	void exitXconst(PostgreSQLParser.XconstContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#bconst}.
	 * @param ctx the parse tree
	 */
	void enterBconst(PostgreSQLParser.BconstContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#bconst}.
	 * @param ctx the parse tree
	 */
	void exitBconst(PostgreSQLParser.BconstContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#fconst}.
	 * @param ctx the parse tree
	 */
	void enterFconst(PostgreSQLParser.FconstContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#fconst}.
	 * @param ctx the parse tree
	 */
	void exitFconst(PostgreSQLParser.FconstContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#iconst}.
	 * @param ctx the parse tree
	 */
	void enterIconst(PostgreSQLParser.IconstContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#iconst}.
	 * @param ctx the parse tree
	 */
	void exitIconst(PostgreSQLParser.IconstContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#sconst}.
	 * @param ctx the parse tree
	 */
	void enterSconst(PostgreSQLParser.SconstContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#sconst}.
	 * @param ctx the parse tree
	 */
	void exitSconst(PostgreSQLParser.SconstContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#anysconst}.
	 * @param ctx the parse tree
	 */
	void enterAnysconst(PostgreSQLParser.AnysconstContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#anysconst}.
	 * @param ctx the parse tree
	 */
	void exitAnysconst(PostgreSQLParser.AnysconstContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#uescape_}.
	 * @param ctx the parse tree
	 */
	void enterUescape_(PostgreSQLParser.Uescape_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#uescape_}.
	 * @param ctx the parse tree
	 */
	void exitUescape_(PostgreSQLParser.Uescape_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#signediconst}.
	 * @param ctx the parse tree
	 */
	void enterSignediconst(PostgreSQLParser.SignediconstContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#signediconst}.
	 * @param ctx the parse tree
	 */
	void exitSignediconst(PostgreSQLParser.SignediconstContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#roleid}.
	 * @param ctx the parse tree
	 */
	void enterRoleid(PostgreSQLParser.RoleidContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#roleid}.
	 * @param ctx the parse tree
	 */
	void exitRoleid(PostgreSQLParser.RoleidContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#rolespec}.
	 * @param ctx the parse tree
	 */
	void enterRolespec(PostgreSQLParser.RolespecContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#rolespec}.
	 * @param ctx the parse tree
	 */
	void exitRolespec(PostgreSQLParser.RolespecContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#role_list}.
	 * @param ctx the parse tree
	 */
	void enterRole_list(PostgreSQLParser.Role_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#role_list}.
	 * @param ctx the parse tree
	 */
	void exitRole_list(PostgreSQLParser.Role_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#colid}.
	 * @param ctx the parse tree
	 */
	void enterColid(PostgreSQLParser.ColidContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#colid}.
	 * @param ctx the parse tree
	 */
	void exitColid(PostgreSQLParser.ColidContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#type_function_name}.
	 * @param ctx the parse tree
	 */
	void enterType_function_name(PostgreSQLParser.Type_function_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#type_function_name}.
	 * @param ctx the parse tree
	 */
	void exitType_function_name(PostgreSQLParser.Type_function_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#nonreservedword}.
	 * @param ctx the parse tree
	 */
	void enterNonreservedword(PostgreSQLParser.NonreservedwordContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#nonreservedword}.
	 * @param ctx the parse tree
	 */
	void exitNonreservedword(PostgreSQLParser.NonreservedwordContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#colLabel}.
	 * @param ctx the parse tree
	 */
	void enterColLabel(PostgreSQLParser.ColLabelContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#colLabel}.
	 * @param ctx the parse tree
	 */
	void exitColLabel(PostgreSQLParser.ColLabelContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#bareColLabel}.
	 * @param ctx the parse tree
	 */
	void enterBareColLabel(PostgreSQLParser.BareColLabelContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#bareColLabel}.
	 * @param ctx the parse tree
	 */
	void exitBareColLabel(PostgreSQLParser.BareColLabelContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#unreserved_keyword}.
	 * @param ctx the parse tree
	 */
	void enterUnreserved_keyword(PostgreSQLParser.Unreserved_keywordContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#unreserved_keyword}.
	 * @param ctx the parse tree
	 */
	void exitUnreserved_keyword(PostgreSQLParser.Unreserved_keywordContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#col_name_keyword}.
	 * @param ctx the parse tree
	 */
	void enterCol_name_keyword(PostgreSQLParser.Col_name_keywordContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#col_name_keyword}.
	 * @param ctx the parse tree
	 */
	void exitCol_name_keyword(PostgreSQLParser.Col_name_keywordContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#type_func_name_keyword}.
	 * @param ctx the parse tree
	 */
	void enterType_func_name_keyword(PostgreSQLParser.Type_func_name_keywordContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#type_func_name_keyword}.
	 * @param ctx the parse tree
	 */
	void exitType_func_name_keyword(PostgreSQLParser.Type_func_name_keywordContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#reserved_keyword}.
	 * @param ctx the parse tree
	 */
	void enterReserved_keyword(PostgreSQLParser.Reserved_keywordContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#reserved_keyword}.
	 * @param ctx the parse tree
	 */
	void exitReserved_keyword(PostgreSQLParser.Reserved_keywordContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#bare_label_keyword}.
	 * @param ctx the parse tree
	 */
	void enterBare_label_keyword(PostgreSQLParser.Bare_label_keywordContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#bare_label_keyword}.
	 * @param ctx the parse tree
	 */
	void exitBare_label_keyword(PostgreSQLParser.Bare_label_keywordContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#any_identifier}.
	 * @param ctx the parse tree
	 */
	void enterAny_identifier(PostgreSQLParser.Any_identifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#any_identifier}.
	 * @param ctx the parse tree
	 */
	void exitAny_identifier(PostgreSQLParser.Any_identifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link PostgreSQLParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(PostgreSQLParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link PostgreSQLParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(PostgreSQLParser.IdentifierContext ctx);
}