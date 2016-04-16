package hadoop.excelmultiout.xlsformat;

import java.io.IOException;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.Iterator;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;

public class XLSParser {
	private static Logger LOG = Logger.getLogger(XLSParser.class);

	// private static final Log LOG = LogFactory.getLog(XLSParser.class);
	// long bytesRead = 0;

	public String parseXLSdata(InputStream is, String FilesName)
			throws IOException {
		StringBuilder currentString = new StringBuilder();
		try {
			@SuppressWarnings("resource")
			HSSFWorkbook workbook = new HSSFWorkbook(is);
			String SheetName = "";
			for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
				//int rowcount = 0;
				HSSFSheet sheet = workbook.getSheetAt(i);
				HSSFRow row;
				HSSFCell cell;
				Iterator<Row> rows = sheet.rowIterator();
				SheetName = sheet.getSheetName();
				while (rows.hasNext()) {
					//rowcount++;
					//if (rowcount == 1)
						//row = (HSSFRow) rows.next();
					//else {
						row = (HSSFRow) rows.next();
						if (sheet.getPhysicalNumberOfRows() != 0) {
							currentString.append("%#@" + FilesName + "_"
									+ SheetName.toString() + "&@!");
						}
						Iterator<Cell> cells = row.cellIterator();
						while (cells.hasNext()) {
							cell = (HSSFCell) cells.next();
							cell.setCellType(Cell.CELL_TYPE_STRING);
							// bytesRead++;
							currentString.append(cell.getStringCellValue()
									.trim() + "|");
							/*
							 * if (cell.getCellType() ==
							 * HSSFCell.CELL_TYPE_STRING) { bytesRead++;
							 * currentString.append(cell.getStringCellValue() +
							 * "|"); } else if (cell.getCellType() ==
							 * HSSFCell.CELL_TYPE_NUMERIC) { bytesRead++;
							 * currentString.append(cell.getNumericCellValue() +
							 * "|"); } else if (cell.getCellType() ==
							 * HSSFCell.CELL_TYPE_BOOLEAN) { bytesRead++;
							 * currentString.append(cell.getBooleanCellValue() +
							 * "|"); }
							 */
						}
						currentString.append("%#@");
						currentString.append("\n");
					//}
				}

			}
		} catch (IOException ex) {
			LOG.error("IO Exception : File not found " + ex.getMessage());

		} catch (Exception e) {
			LOG.error("Error in XLS File : " + e.getMessage());

		} finally {
			is.close();
		}

		return currentString.toString();

	}

	/*
	 * public long getBytesRead() { return bytesRead; }
	 */

}