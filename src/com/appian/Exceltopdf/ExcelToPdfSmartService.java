package com.appian.Exceltopdf;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Logger;

import com.appiancorp.suiteapi.common.Name;
import com.appiancorp.suiteapi.common.exceptions.ErrorCode;
import com.appiancorp.suiteapi.content.ContentConstants;
import com.appiancorp.suiteapi.content.ContentService;
import com.appiancorp.suiteapi.knowledge.Document;
import com.appiancorp.suiteapi.knowledge.DocumentDataType;
import com.appiancorp.suiteapi.knowledge.FolderDataType;
import com.appiancorp.suiteapi.process.exceptions.SmartServiceException;
import com.appiancorp.suiteapi.process.framework.AppianSmartService;
import com.appiancorp.suiteapi.process.framework.Input;
import com.appiancorp.suiteapi.process.framework.Order;
import com.appiancorp.suiteapi.process.framework.Required;
import com.appiancorp.suiteapi.process.palette.PaletteInfo;

import com.webco.master.MasterPlugin;

@PaletteInfo(paletteCategory = "Appian Smart Services", palette = "Document Generation")
@Order({ "SourceDocument", "NewDocumentName", "SaveInFolder" })
public class ExcelToPdfSmartService extends AppianSmartService {

    private static final Logger LOG =
            Logger.getLogger(ExcelToPdfSmartService.class.getName());

    private static final long MAX_FILE_SIZE = 10 * 1024 * 1024;
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_WAIT = 3000; // 3 sec
    private static final long TIMEOUT = 30000;   // 30 sec

    private final ContentService contentService;

    private Long SourceDocument;
    private Long NewDocumentName;
    private Long SaveInFolder;

    public ExcelToPdfSmartService(ContentService contentService) {
        this.contentService = contentService;
    }

    @Override
    public void run() throws SmartServiceException {

        File inputExcel = null;
        File outputPdf = null;

        try {

            LOG.info("Excel → PDF conversion started");

            validateInputs();

            /* -----------------------------
               1. Download Excel
               ----------------------------- */
            Document excelDoc = contentService.download(
                    SourceDocument,
                    ContentConstants.VERSION_CURRENT,
                    false
            )[0];

            if (excelDoc == null) {
                throw new SmartServiceException(
                        ErrorCode.GENERIC_ERROR,
                        "Source document not found"
                );
            }

            validateExtension(excelDoc);

            inputExcel = excelDoc.accessAsReadOnlyFile();

            if (inputExcel == null || !inputExcel.exists()) {
                throw new SmartServiceException(
                        ErrorCode.GENERIC_ERROR,
                        "Unable to access Excel file"
                );
            }

            validateFileSize(inputExcel);

            /* -----------------------------
               2. Prepare Output PDF
               ----------------------------- */
            outputPdf = File.createTempFile("excel_to_pdf_convert", ".pdf");
            Path outputPath = outputPdf.toPath();

            /* -----------------------------
               3. Convert with Retry + Timeout
               ----------------------------- */
            boolean success = false;

            for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {

                try {

                    LOG.info("Attempt " + attempt + " of " + MAX_RETRIES);

                    long startTime = System.currentTimeMillis();

                    final File finalInput = inputExcel;
                    final Path finalOutput = outputPath;

                    Thread conversionThread = new Thread(() -> {
                        try {
                            MasterPlugin plugin = new MasterPlugin();
                            plugin.excelToPdf.convert(finalInput, finalOutput);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });

                    conversionThread.start();
                    conversionThread.join(TIMEOUT);

                    if (conversionThread.isAlive()) {
                        conversionThread.interrupt();
                        throw new RuntimeException("Conversion timed out");
                    }

                    long endTime = System.currentTimeMillis();

                    LOG.info("Conversion successful in " + (endTime - startTime) + " ms");

                    success = true;
                    break;

                } catch (Exception e) {

                    LOG.warning("Attempt " + attempt + " failed: " + e.getMessage());

                    if (attempt == MAX_RETRIES) {
                        throw new SmartServiceException(
                                ErrorCode.GENERIC_ERROR,
                                "Conversion failed after retries",
                                e
                        );
                    }

                    try {
                        LOG.info("Retrying in " + RETRY_WAIT + " ms...");
                        Thread.sleep(RETRY_WAIT);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            /* -----------------------------
               4. Create Appian Document
               ----------------------------- */
            Document newDoc = new Document();
            newDoc.setName("converted.pdf");
            newDoc.setExtension("pdf");
            newDoc.setParent(NewDocumentName);
            newDoc.setSize((int) outputPdf.length());

            Long newDocId = contentService.create(newDoc, ContentConstants.UNIQUE_NONE);

            Document createdDoc = contentService.download(
                    newDocId,
                    ContentConstants.VERSION_CURRENT,
                    false
            )[0];

            /* -----------------------------
               5. Upload PDF
               ----------------------------- */
            try (OutputStream out = createdDoc.getOutputStream()) {
                Files.copy(outputPdf.toPath(), out);
            }

            SaveInFolder = contentService
                    .getVersion(newDocId, ContentConstants.VERSION_CURRENT)
                    .getId();

            LOG.info("Excel → PDF conversion completed successfully");

        } catch (Exception e) {

            LOG.severe("Error: " + e.getMessage());

            throw new SmartServiceException(
                    ErrorCode.GENERIC_ERROR,
                    "Failed to convert Excel to PDF",
                    e
            );

        } finally {

            cleanupTempFile(outputPdf);

            if (inputExcel != null && inputExcel.exists()) {
                inputExcel.delete();
            }
        }
    }

    /* ================================
       Validation
       ================================ */

    private void validateInputs() throws SmartServiceException {

        if (SourceDocument == null) {
            throw new SmartServiceException(ErrorCode.GENERIC_ERROR, "Source document required");
        }

        if (NewDocumentName == null) {
            throw new SmartServiceException(ErrorCode.GENERIC_ERROR, "Target folder required");
        }
    }

    private void validateFileSize(File file) throws SmartServiceException {

        if (file.length() > MAX_FILE_SIZE) {
            throw new SmartServiceException(
                    ErrorCode.GENERIC_ERROR,
                    "File too large (Max 10MB)"
            );
        }
    }

    private void validateExtension(Document doc) throws SmartServiceException {

        String name = doc.getName().toLowerCase();

        if (!name.endsWith(".xlsx")) {
            throw new SmartServiceException(
                    ErrorCode.GENERIC_ERROR,
                    "Source must be Excel file(.Xlsx)"
            );
        }
    }

    private void cleanupTempFile(File file) {

        if (file != null && file.exists()) {
            try {
                Files.delete(file.toPath());
            } catch (Exception e) {
                LOG.warning("Failed to delete temp file");
            }
        }
    }

    /* ================================
       Inputs
       ================================ */

    @Input(required = Required.ALWAYS)
    @Name("SourceDocument")
    @DocumentDataType
    public void setSourceDocument(Long sourceDocument) {
        this.SourceDocument = sourceDocument;
    }

    @Input(required = Required.ALWAYS)
    @Name("TargetFolder")
    @FolderDataType
    public void setTargetFolder(Long targetFolder) {
        this.NewDocumentName = targetFolder;
    }

    /* ================================
       Output
       ================================ */

    @Name("NewDocumentCreated")
    @DocumentDataType
    public Long getNewDocumentCreated() {
        return SaveInFolder;
    }
}