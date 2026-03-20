package com.pm.appointmentservice.service;

import com.pm.appointmentservice.dto.AppointmentResponseDto;
import com.pm.appointmentservice.repository.AppointmentRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@Service
public class AppointmentService {

    private final AppointmentRepository appointmentRepository;

    public AppointmentService(AppointmentRepository appointmentRepository) {
        this.appointmentRepository = appointmentRepository;
    }

    public List<AppointmentResponseDto> getAppointmentByDateRange(
            LocalDateTime from, LocalDateTime to
    ){
        return appointmentRepository.findByStartTimeBetween(from, to).stream()
                .map(appointment -> {
                    AppointmentResponseDto appointmentResponseDto = new AppointmentResponseDto();
                    appointmentResponseDto.setId(appointment.getId());
                    appointmentResponseDto.setPatientId(appointment.getPatientId());
                    appointmentResponseDto.setStartTime(appointment.getStartTime());
                    appointmentResponseDto.setEndTime(appointment.getEndTime());
                    appointmentResponseDto.setReason(appointment.getReason());
                    appointmentResponseDto.setVersion(appointment.getVersion());
                    return appointmentResponseDto;
                }).toList();
    }
}
