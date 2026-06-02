package com.karix.dlrreceiver.exception;

import jakarta.validation.ConstraintViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.HttpRequestMethodNotSupportedException;

import com.karix.commonutil.model.StatusCode;

import java.util.HashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final String STATUS_CODE = "statusCode";
    private static final String MESSAGE = "message";
    private static final String ERRORS = "errors";

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Object> handleValidationException(MethodArgumentNotValidException ex) {

        Map<String, Object> response = new HashMap<>();
        Map<String, String> fieldErrors = new HashMap<>();

        for (FieldError error : ex.getBindingResult().getFieldErrors()) {

            String message = error.getDefaultMessage();

            if (message == null || message.isBlank()) {
                message = error.getField() + " is invalid";
            }

            fieldErrors.put(error.getField(), message);
        }

        StatusCode statusCode = StatusCode.INVALID_REQUEST;

        response.put(STATUS_CODE, statusCode.getCode());
        response.put(MESSAGE, statusCode.getDescription());
        response.put(ERRORS, fieldErrors);

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    /**
     * Handles malformed JSON
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Object> handleInvalidJson(HttpMessageNotReadableException ex) {

        Map<String, Object> response = new HashMap<>();

        StatusCode statusCode = StatusCode.INVALID_JSON;

        response.put(STATUS_CODE, statusCode.getCode());
        response.put(MESSAGE, statusCode.getDescription());

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    /**
     * Handles query param and path param validation failures
     */
    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<Object> handleConstraintViolation(ConstraintViolationException ex) {

        Map<String, Object> response = new HashMap<>();
        Map<String, String> errors = new HashMap<>();

        ex.getConstraintViolations().forEach(violation ->
                errors.put(
                        violation.getPropertyPath().toString(),
                        violation.getMessage()
                )
        );

        StatusCode statusCode = StatusCode.INVALID_REQUEST;

        response.put(STATUS_CODE, statusCode.getCode());
        response.put(MESSAGE, statusCode.getDescription());
        response.put(ERRORS, errors);

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    /**
     * Handles ResponseStatusException
     */
    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<Object> handleResponseStatusException(ResponseStatusException ex) {

        Map<String, Object> response = new HashMap<>();

        StatusCode statusCode = StatusCode.INVALID_REQUEST;

        response.put(STATUS_CODE, statusCode.getCode());
        response.put(MESSAGE, ex.getReason());

        return ResponseEntity.status(ex.getStatusCode()).body(response);
    }

    /**
     * Handles invalid HTTP method (405)
     */
    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<Object> handleMethodNotSupported(HttpRequestMethodNotSupportedException ex) {

        Map<String, Object> response = new HashMap<>();

        StatusCode statusCode = StatusCode.INVALID_REQUEST;

        response.put(STATUS_CODE, statusCode.getCode());
        response.put(MESSAGE, statusCode.getDescription());

        return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).body(response);
    }

    /**
     * Fallback handler
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Object> handleGenericException(Exception ex) {

        Map<String, Object> response = new HashMap<>();

        StatusCode statusCode = StatusCode.INVALID_REQUEST;

        response.put(STATUS_CODE, statusCode.getCode());
        response.put(MESSAGE, statusCode.getDescription());

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}
