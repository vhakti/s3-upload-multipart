import {
  S3Client,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  CompletedPart,
} from "@aws-sdk/client-s3";
import * as fs from "fs";
import * as path from "path";

// Configuración de credenciales y región
const REGION = "us-east-1"; // Cambia a tu región
 

const BUCKET_NAME = "challengys3";
const FILE_NAME = "2023-archivo-grande.mkv";
const FILE_PATH = path.resolve(__dirname, "D://Temp/2023-archivo-grande.mkv");

const PART_SIZE = 5 * 1024 * 1024; // 5 MB mínimo

// Inicialización del cliente S3 con credenciales y configuración explícita
const s3Client = new S3Client({
  region: REGION  // Signature Version 4 es la predeterminada en SDK v3,
  // pero si quieres forzarla explícitamente, puedes usar middleware o configuración avanzada.
  // Aquí no es necesario configurarla explícitamente porque es default.
});

async function multipartUpload() {
  try {
    // 1. Iniciar carga multiparte
    const createMultipartUploadCommand = new CreateMultipartUploadCommand({
      Bucket: BUCKET_NAME,
      Key: FILE_NAME,
    });

    const createMultipartUploadResponse = await s3Client.send(createMultipartUploadCommand);
    const uploadId = createMultipartUploadResponse.UploadId;

    if (!uploadId) {
      throw new Error("No se pudo obtener el UploadId para la carga multiparte.");
    }

    console.log(`Carga multiparte iniciada. UploadId: ${uploadId}`);

    // 2. Subir partes
    const fileSize = fs.statSync(FILE_PATH).size;
    let partNumber = 1;
    let start = 0;
    const uploadedParts: CompletedPart[] = [];

    while (start < fileSize) {
      const end = Math.min(start + PART_SIZE, fileSize);
      const partLength = end - start;

      const partStream = fs.createReadStream(FILE_PATH, {
        start,
        end: end - 1, // El rango es inclusivo
      });

      console.log(`Subiendo parte ${partNumber}, bytes ${start} a ${end - 1}`);

      const uploadPartCommand = new UploadPartCommand({
        Bucket: BUCKET_NAME,
        Key: KEY_NAME,
        UploadId: uploadId,
        PartNumber: partNumber,
        Body: partStream,
        ContentLength: partLength,
      });

      const uploadPartResponse = await s3Client.send(uploadPartCommand);

      if (!uploadPartResponse.ETag) {
        throw new Error(`No se recibió ETag para la parte ${partNumber}`);
      }

      uploadedParts.push({
        ETag: uploadPartResponse.ETag,
        PartNumber: partNumber,
      });

      console.log(`Parte ${partNumber} subida correctamente. ETag: ${uploadPartResponse.ETag}`);

      partNumber++;
      start += PART_SIZE;
    }

    // 3. Completar la carga multiparte
    const completeMultipartUploadCommand = new CompleteMultipartUploadCommand({
      Bucket: BUCKET_NAME,
      Key: KEY_NAME,
      UploadId: uploadId,
      MultipartUpload: {
        Parts: uploadedParts,
      },
    });

    const completeResponse = await s3Client.send(completeMultipartUploadCommand);

    console.log("Carga multiparte completada exitosamente.");
    console.log(`Ubicación del objeto: ${completeResponse.Location}`);
  } catch (error) {
    console.error("Error durante la carga multiparte:", error);

    // Si hay uploadId, abortar la carga para liberar recursos
    if (error instanceof Error && (error as any).uploadId) {
      const uploadId = (error as any).uploadId;
      const abortMultipartUploadCommand = new AbortMultipartUploadCommand({
        Bucket: BUCKET_NAME,
        Key: KEY_NAME,
        UploadId: uploadId,
      });
      await s3Client.send(abortMultipartUploadCommand);
      console.log("Carga multiparte abortada.");
    }
  }
}

// Ejecutar la función
multipartUpload().catch((error) => {
  console.error("Error inesperado:", error);
});
