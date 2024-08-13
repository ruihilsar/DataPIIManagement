package data.cascade;

import java.util.List;
import java.util.Map;

public interface CascadeService {

	/**
     * To check if the column is a primary key of the table.
     *
     * @return true: is a part of the primary key, false: is not a primary key
     */
	public boolean isPrimaryKey();
	
	/**
     * To check if the column is a foreign key of the table.
     *
     * @return true: is a part of the foreign key, false: is not a foreign key
     */
    public boolean isForeignKey();
	
    /**
     * return a list of current table's column(s) for the given table and column into a format of List<String>.
     * e.g. if the given primary key field is:
     * 			rm.rm_id, 
     * 		it should return the current table's column(s):
     * 			bl_id, fl_id, rm_id.
     *
     * @return List<String> list of field name(s).
     */
    public List<String> getPrimaryKeyCurrentColumns();
    
    /**
     * return a list of current table's column(s) for the given table and column into a format of List<String>.
     * e.g. if the given foreign key field is:
     * 			mo_ta.from_fl_id, 
     * 		it should return the current table's column(s):
     * 			from_bl_id, from_fl_id.
     *
     * @return List<String> list of field name(s).
     */
    public List<String> getForeignKeyCurrentColumns();
    
    /**
     * return a reference table and column(s) for the given table and column(s) into a format of Map<table_name, list of field column(s)>.
     * e.g. if the given foreign key field is:
     * 			mo_ta.from_fl_id, 
     * 		it should return the reference table and column(s):
     * 			fl.(bl_id, fl_id).
     *
     * @return Map<String, List<String>> one pair of reference table name and field name(s). the size of this return map should always be 1.
     */
    public Map<String, List<String>> getForeignKeyReferenceTableColumns();
    
    /**
     * create a new row of record in the reference table with a new primary key value.
     * e.g. if the reference table and column is:
     * 			em.em_id = "AFM"
     * 		it will insert a new record with a new value:
     * 			em.em_id = "AFMXXX"
     */
    public void createNewReferenceRecord();
    
    /**
     * return a list of foreign keys based on the given reference table name.
     * e.g. if the reference table is fl, it should return a list of table records using fl as a foreign key
     * 		this can be one of the record in the list:
     * 			Map<mo_ta, List<from_bl_id, from_fl_id>>.
     *
     * @return List<Map<String, List<String>>> foreignKeyList
     */
    public List<Map<String, List<String>>> getForeignKeyList();
    
    /**
     * To update all of the foreign key record if found.
     */
    public void updateForeignKeys();
    
    /**
     * To remove the old reference table record after updating it with the new value.
     */
    public void deleteOldReferenceRecord();
}
