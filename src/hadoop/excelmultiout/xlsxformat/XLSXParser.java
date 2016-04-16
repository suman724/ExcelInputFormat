package hadoop.excelmultiout.xlsxformat;

import java.io.IOException;
import org.apache.log4j.Logger;
import java.io.InputStream;
import java.util.Iterator;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class XLSXParser {
	private static Logger LOG = Logger.getLogger(XLSXParser.class);

	// private static final Log LOG = LogFactory.getLog(XLSXParser.class);
	// long bytesRead = 0;

	public String parseXLSXdata(InputStream is, String FilesName)
			throws IOException {
		StringBuilder currentString = new StringBuilder();
		try {
			@SuppressWarnings("resource")
			XSSFWorkbook workbook = new XSSFWorkbook(is);
			String SheetName = "";
			for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
				//int rowcount = 0;
				XSSFSheet sheet = workbook.getSheetAt(i);
				XSSFRow row;
				XSSFCell cell;
				Iterator<Row> rows = sheet.rowIterator();
				SheetName = sheet.getSheetName();
				while (rows.hasNext()) {
					//rowcount++;
					//if (rowcount == 1)
						//row = (XSSFRow) rows.next();
					//else {
						row = (XSSFRow) rows.next();
						if (sheet.getPhysicalNumberOfRows() != 0) {
							currentString.append("%#@" + FilesName + "_"
									+ SheetName.toString() + "&@!");
						}
						Iterator<Cell> cells = row.cellIterator();
						while (cells.hasNext()) {
							cell = (XSSFCell) cells.next();
							cell.setCellType(Cell.CELL_TYPE_STRING);
							// bytesRead++;
							currentString.append(cell.getStringCellValue()
									.trim() + "|");
							/*
							 * if (cell.getCellType() ==
							 * XSSFCell.CELL_TYPE_STRING) { bytesRead++;
							 * currentString.append(cell.getStringCellValue() +
							 * "|"); } else if (cell.getCellType() ==
							 * XSSFCell.CELL_TYPE_NUMERIC) { bytesRead++;
							 * currentString.append(cell.getNumericCellValue() +
							 * "|"); } else if (cell.getCellType() ==
							 * XSSFCell.CELL_TYPE_BOOLEAN) { bytesRead++;
							 * currentString.append(cell.getBooleanCellValue() +
							 * "|"); }
							 */
						}
						currentString.append("%#@");
						currentString.append("\n");
					//}
				}

			}
		} catch (IOException e) {
			LOG.error("IO Exception : File not found " + e.getMessage());

		} catch (Exception e) {
			LOG.error("Error in XLSX File : " + e.getMessage());

		} finally {
			is.close();
		}

		return currentString.toString();

	}

	/*
	 * public long getBytesRead() { return bytesRead; }
	 */

}